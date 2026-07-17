package com.flam.queuectl.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Tracks worker liveness (for `status`) and carries the cross-process
 * stop signal (for `worker stop`).
 *
 * WHY heartbeats instead of just a registration row: a worker killed with
 * kill -9 never gets to deregister. If `status` trusted registration rows,
 * dead workers would show as active forever. Instead, each worker touches
 * last_heartbeat every poll cycle, and "active" is defined as
 * "heartbeat within the last STALE_AFTER window". Rows are self-expiring
 * facts, not permanent claims — the standard liveness pattern (same idea as
 * Kubernetes liveness probes or consumer-group heartbeats in Kafka).
 *
 * WHY the stop flag lives in the DB: `worker stop` runs in a DIFFERENT
 * process from the workers. A DB row is the simplest portable cross-process
 * channel — no PID files, no OS signals (which differ between Windows and
 * Unix, and Tanishka develops on Windows), no sockets. Workers poll the
 * flag between jobs, which is exactly the granularity graceful shutdown
 * needs: never interrupt a running job, never start a new one.
 */
public final class WorkerRegistry {

    /** A worker whose heartbeat is older than this is considered dead. */
    public static final Duration STALE_AFTER = Duration.ofSeconds(10);

    private static final String STOP_KEY = "stop_requested";

    public record WorkerInfo(String id, long pid, String startedAt, String lastHeartbeat, String currentJob) { }

    public static void register(String workerId, long pid) {
        String now = Instant.now().toString();
        exec("INSERT INTO workers(id, pid, started_at, last_heartbeat) VALUES(?,?,?,?) " +
             "ON CONFLICT(id) DO UPDATE SET last_heartbeat = excluded.last_heartbeat",
             workerId, pid, now, now);
    }

    public static void heartbeat(String workerId, String currentJob) {
        exec("UPDATE workers SET last_heartbeat = ?, current_job = ? WHERE id = ?",
             Instant.now().toString(), currentJob, workerId);
    }

    public static void deregister(String workerId) {
        exec("DELETE FROM workers WHERE id = ?", workerId);
    }

    public static List<WorkerInfo> activeWorkers() {
        String cutoff = Instant.now().minus(STALE_AFTER).toString();
        List<WorkerInfo> out = new ArrayList<>();
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id, pid, started_at, last_heartbeat, current_job " +
                     "FROM workers WHERE last_heartbeat >= ? ORDER BY id")) {
            ps.setString(1, cutoff);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new WorkerInfo(rs.getString(1), rs.getLong(2),
                            rs.getString(3), rs.getString(4), rs.getString(5)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("activeWorkers query failed", e);
        }
        return out;
    }

    /** Remove rows from workers that died without deregistering. */
    public static void pruneStale() {
        String cutoff = Instant.now().minus(STALE_AFTER).toString();
        exec("DELETE FROM workers WHERE last_heartbeat < ?", cutoff);
    }

    // ---- stop signal ----

    public static void requestStop() {
        exec("INSERT INTO control(key, value) VALUES(?, '1') " +
             "ON CONFLICT(key) DO UPDATE SET value = '1'", STOP_KEY);
    }

    /** Cleared on `worker start` so a previous stop doesn't kill new workers instantly. */
    public static void clearStop() {
        exec("DELETE FROM control WHERE key = ?", STOP_KEY);
    }

    public static boolean stopRequested() {
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement("SELECT value FROM control WHERE key = ?")) {
            ps.setString(1, STOP_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && "1".equals(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("stopRequested query failed", e);
        }
    }

    private static void exec(String sql, Object... params) {
        try (Connection c = Database.open(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("worker registry write failed: " + sql, e);
        }
    }

    private WorkerRegistry() { }
}
