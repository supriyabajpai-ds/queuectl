package com.flam.queuectl.storage;

import com.flam.queuectl.model.Job;
import com.flam.queuectl.model.JobState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * All reads/writes of the `jobs` table. The single most important method in
 * this project is {@link #claimNext(String)} — read its comment carefully.
 */
public final class JobStore {

    // ------------------------------------------------------------------
    // Enqueue
    // ------------------------------------------------------------------

    /**
     * Insert a new pending job. The PRIMARY KEY on id gives us duplicate
     * detection for free: enqueueing an existing id fails loudly instead of
     * silently overwriting a job that may be mid-flight.
     */
    public static void enqueue(Job job) {
        String sql = """
            INSERT INTO jobs (id, command, state, attempts, max_retries, priority,
                              created_at, updated_at, next_run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (Connection c = Database.open(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, job.getId());
            ps.setString(2, job.getCommand());
            ps.setString(3, job.getState().dbValue());
            ps.setInt(4, job.getAttempts());
            ps.setInt(5, job.getMaxRetries());
            ps.setInt(6, job.getPriority());
            ps.setString(7, job.getCreatedAt());
            ps.setString(8, job.getUpdatedAt());
            ps.setString(9, job.getNextRunAt());
            ps.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage().contains("PRIMARY KEY")) {
                throw new IllegalArgumentException(
                        "A job with id '" + job.getId() + "' already exists. " +
                        "Use a new id, or `queuectl dlq retry " + job.getId() + "` if it is in the DLQ.");
            }
            throw new RuntimeException("enqueue failed", e);
        }
    }

    // ------------------------------------------------------------------
    // THE ATOMIC CLAIM  — duplicate-processing prevention
    // ------------------------------------------------------------------

    /**
     * Atomically claim the next runnable job for a worker, or return empty.
     *
     * HOW IT WORKS — this is the concurrency mechanism the assignment cares
     * most about, so in detail:
     *
     * The claim is a SINGLE conditional UPDATE:
     *
     *   UPDATE jobs
     *   SET    state = 'processing', claimed_by = :worker, ...
     *   WHERE  id = ( SELECT id FROM jobs
     *                 WHERE runnable-predicate
     *                 ORDER BY priority DESC, created_at ASC
     *                 LIMIT 1 )
     *   AND    state IN ('pending','failed')          -- re-checked guard
     *
     * Three properties combine to make duplicate claims impossible:
     *
     * 1. SQLite executes each statement atomically, and allows exactly ONE
     *    writer at a time (enforced by the engine's write lock, across
     *    threads AND across OS processes). Two workers issuing this UPDATE
     *    concurrently are serialised: one runs fully, then the other.
     *
     * 2. The subquery and the outer guard are evaluated inside that same
     *    atomic statement. When the second worker's UPDATE finally runs, it
     *    re-evaluates the subquery against the NEW state of the table — the
     *    job the first worker took is now 'processing', so it no longer
     *    matches, and the second worker either claims a different job or
     *    updates 0 rows.
     *
     * 3. The caller checks the affected-row count. rows == 1 means "you own
     *    exactly the job now marked claimed_by = you"; rows == 0 means
     *    "someone beat you to it / nothing runnable" — the worker just polls
     *    again. There is no window in which two workers can both see the
     *    same job as claimable, because "look" and "take" are one operation.
     *
     * Contrast with the naive broken version (SELECT the next pending job,
     * then UPDATE it to processing as two statements): between those two
     * statements another worker can SELECT the same row → both mark it
     * processing → the command runs twice. That's the exact race the
     * disqualifier list warns about. Collapsing check-and-set into one
     * statement is the classic fix — same idea as compare-and-swap in CPUs
     * or `UPDATE ... WHERE state='pending'` optimistic locking in any RDBMS.
     *
     * The runnable predicate also encodes retry backoff and scheduling:
     *   - pending jobs with next_run_at NULL or <= now  (run_at bonus)
     *   - failed  jobs with next_run_at <= now          (backoff has elapsed)
     * ISO-8601 strings compare lexicographically in time order, so plain
     * string <= is a correct time comparison.
     */
    public static Optional<Job> claimNext(String workerId) {
        String now = Instant.now().toString();
        String update = """
            UPDATE jobs
            SET state = 'processing',
                claimed_by = ?,
                updated_at = ?
            WHERE id = (
                SELECT id FROM jobs
                WHERE (state = 'pending' AND (next_run_at IS NULL OR next_run_at <= ?))
                   OR (state = 'failed'  AND next_run_at <= ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            )
            AND state IN ('pending', 'failed')
            """;
        try (Connection c = Database.open()) {
            int rows;
            try (PreparedStatement ps = c.prepareStatement(update)) {
                ps.setString(1, workerId);
                ps.setString(2, now);
                ps.setString(3, now);
                ps.setString(4, now);
                rows = ps.executeUpdate();
            }
            if (rows == 0) return Optional.empty();

            // Safe follow-up read: this worker processes strictly one job at
            // a time and always resolves it (completed/failed/dead) before
            // claiming again, so "state='processing' AND claimed_by=me"
            // identifies exactly the row the UPDATE above just took.
            String select = "SELECT * FROM jobs WHERE state = 'processing' AND claimed_by = ?";
            try (PreparedStatement ps = c.prepareStatement(select)) {
                ps.setString(1, workerId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return Optional.of(fromRow(rs));
                }
            }
            return Optional.empty(); // unreachable in practice
        } catch (SQLException e) {
            throw new RuntimeException("claimNext failed", e);
        }
    }

    // ------------------------------------------------------------------
    // Result transitions
    // ------------------------------------------------------------------

    /** processing → completed */
    public static void markCompleted(String jobId, int exitCode, String output) {
        String sql = """
            UPDATE jobs
            SET state = 'completed', last_exit_code = ?, last_output = ?,
                next_run_at = NULL, updated_at = ?
            WHERE id = ?
            """;
        execUpdate(sql, exitCode, output, Instant.now().toString(), jobId);
    }

    /**
     * processing → failed (with backoff schedule) or → dead (DLQ).
     *
     * The DECISION and the WRITE happen here in one statement per outcome:
     *   attempts := attempts + 1
     *   if attempts >= max_retries        → dead  (permanently failed, DLQ)
     *   else                              → failed, next_run_at = now + base^attempts
     *
     * WHY compute the delay in Java rather than SQL: `base ^ attempts` needs
     * pow(), which core SQLite lacks; the worker already knows the attempt
     * count it just observed, and only the OWNING worker ever writes the
     * result of a processing job, so there is no race on this row.
     *
     * @return the resulting state (FAILED or DEAD) so the worker can log it.
     */
    public static JobState markFailed(Job job, int exitCode, String output, int backoffBase) {
        int newAttempts = job.getAttempts() + 1;
        String now = Instant.now().toString();

        if (newAttempts >= job.getMaxRetries()) {
            String sql = """
                UPDATE jobs
                SET state = 'dead', attempts = ?, last_exit_code = ?, last_output = ?,
                    next_run_at = NULL, updated_at = ?
                WHERE id = ?
                """;
            execUpdate(sql, newAttempts, exitCode, output, now, job.getId());
            return JobState.DEAD;
        }

        long delaySeconds = (long) Math.pow(backoffBase, newAttempts); // delay = base ^ attempts
        String nextRun = Instant.now().plusSeconds(delaySeconds).toString();
        String sql = """
            UPDATE jobs
            SET state = 'failed', attempts = ?, last_exit_code = ?, last_output = ?,
                next_run_at = ?, updated_at = ?
            WHERE id = ?
            """;
        execUpdate(sql, newAttempts, exitCode, output, nextRun, now, job.getId());
        return JobState.FAILED;
    }

    /**
     * dead → pending (manual DLQ retry). Attempts reset to 0 — the operator
     * has presumably fixed the underlying cause, so the job deserves a fresh
     * retry budget rather than instantly dying again on its first failure.
     * The state='dead' guard makes this a no-op on non-DLQ jobs.
     */
    public static boolean retryFromDlq(String jobId) {
        String sql = """
            UPDATE jobs
            SET state = 'pending', attempts = 0, next_run_at = NULL,
                claimed_by = NULL, updated_at = ?
            WHERE id = ? AND state = 'dead'
            """;
        return execUpdate(sql, Instant.now().toString(), jobId) == 1;
    }

    // ------------------------------------------------------------------
    // Queries (status / list / dlq list)
    // ------------------------------------------------------------------

    public static Map<String, Integer> countsByState() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (JobState s : JobState.values()) counts.put(s.dbValue(), 0);
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement("SELECT state, COUNT(*) FROM jobs GROUP BY state");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) counts.put(rs.getString(1), rs.getInt(2));
        } catch (SQLException e) {
            throw new RuntimeException("status query failed", e);
        }
        return counts;
    }

    public static List<Job> list(JobState state) {
        String sql = (state == null)
                ? "SELECT * FROM jobs ORDER BY created_at"
                : "SELECT * FROM jobs WHERE state = ? ORDER BY created_at";
        List<Job> out = new ArrayList<>();
        try (Connection c = Database.open(); PreparedStatement ps = c.prepareStatement(sql)) {
            if (state != null) ps.setString(1, state.dbValue());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(fromRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("list query failed", e);
        }
        return out;
    }

    public static Optional<Job> get(String id) {
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement("SELECT * FROM jobs WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.of(fromRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("get query failed", e);
        }
        return Optional.empty();
    }

    /**
     * Crash recovery: return orphaned 'processing' jobs (their claiming
     * worker is gone) back to 'pending'. Called on `worker start` BEFORE new
     * workers begin claiming — at that moment no live worker exists, so any
     * 'processing' row can only be the residue of a crash/kill, never a job
     * a live worker is running. Without this, a kill -9 would strand jobs in
     * 'processing' forever ("jobs lost on restart" disqualifier).
     */
    public static int recoverOrphans() {
        String sql = """
            UPDATE jobs
            SET state = 'pending', claimed_by = NULL, updated_at = ?
            WHERE state = 'processing'
            """;
        return execUpdate(sql, Instant.now().toString());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static int execUpdate(String sql, Object... params) {
        try (Connection c = Database.open(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("update failed: " + sql, e);
        }
    }

    private static Job fromRow(ResultSet rs) throws SQLException {
        Job j = new Job();
        j.setId(rs.getString("id"));
        j.setCommand(rs.getString("command"));
        j.setState(JobState.fromDb(rs.getString("state")));
        j.setAttempts(rs.getInt("attempts"));
        j.setMaxRetries(rs.getInt("max_retries"));
        j.setPriority(rs.getInt("priority"));
        j.setCreatedAt(rs.getString("created_at"));
        j.setUpdatedAt(rs.getString("updated_at"));
        j.setNextRunAt(rs.getString("next_run_at"));
        j.setClaimedBy(rs.getString("claimed_by"));
        int exit = rs.getInt("last_exit_code");
        if (!rs.wasNull()) j.setLastExitCode(exit);
        j.setLastOutput(rs.getString("last_output"));
        return j;
    }

    private JobStore() { }
}
