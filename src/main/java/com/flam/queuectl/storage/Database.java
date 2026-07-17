package com.flam.queuectl.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Owns the SQLite connection and schema. Everything below this class speaks
 * plain JDBC against these tables.
 *
 * WHY SQLite (vs a JSON file):
 *  1. Atomic multi-process writes. Several worker processes and CLI commands
 *     hit the store concurrently. SQLite gives us ACID transactions and a
 *     single writer lock at the engine level — with a JSON file we would have
 *     to build file locking, atomic rename, and corruption recovery by hand,
 *     and that is exactly where "race conditions / duplicate execution"
 *     disqualifiers come from.
 *  2. The atomic job claim (see JobStore.claimNext) is ONE conditional
 *     UPDATE statement. SQLite executes each statement atomically, so the
 *     claim needs no application-level mutex and works even across separate
 *     OS processes — a Java `synchronized` block could never do that, since
 *     it only protects threads inside one JVM.
 *  3. Queryability: `list --state failed` is a WHERE clause, not a full-file
 *     scan and rewrite.
 *
 * WHY WAL + busy_timeout pragmas:
 *  - WAL (write-ahead logging) lets readers proceed while a writer commits,
 *    which matters because `status`/`list` run while workers write.
 *  - busy_timeout=5000 makes a connection WAIT up to 5s for the write lock
 *    instead of instantly throwing SQLITE_BUSY. Under N workers polling the
 *    same table this converts lock contention from an error into a short wait.
 *
 * WHY a connection per operation (no pool): every CLI invocation is a
 * short-lived process, and each worker thread opens its own connection.
 * SQLite connections are cheap local file handles; a pool would add
 * complexity with zero benefit here. Each thread having its OWN connection
 * also avoids sharing a JDBC Connection across threads, which the driver
 * does not guarantee to be safe.
 */
public final class Database {

    private Database() { }

    /** Data lives in $QUEUECTL_HOME or ~/.queuectl — overridable so tests can isolate state. */
    public static Path dataDir() {
        String override = System.getenv("QUEUECTL_HOME");
        Path dir = (override != null && !override.isBlank())
                ? Paths.get(override)
                : Paths.get(System.getProperty("user.home"), ".queuectl");
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            throw new RuntimeException("Cannot create data directory " + dir, e);
        }
        return dir;
    }

    public static String jdbcUrl() {
        return "jdbc:sqlite:" + dataDir().resolve("queuectl.db");
    }

    /** Open a connection with the pragmas applied and the schema guaranteed. */
    public static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(jdbcUrl());
        try (Statement st = c.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA busy_timeout=5000");
            st.execute("PRAGMA synchronous=NORMAL"); // safe with WAL; much faster than FULL
        }
        ensureSchema(c);
        return c;
    }

    /**
     * CREATE TABLE IF NOT EXISTS keeps this idempotent — every process that
     * opens the DB guarantees the schema, so there is no separate "migrate"
     * step to forget.
     *
     * Schema notes:
     *  - jobs.state is TEXT with a CHECK constraint: the DB itself rejects
     *    an impossible state, mirroring the JobState enum at the storage layer.
     *  - Timestamps are ISO-8601 UTC TEXT. ISO-8601 sorts lexicographically
     *    in time order, so `next_run_at <= ?` string comparison is correct —
     *    that one property lets backoff scheduling be a plain WHERE clause.
     *  - dlq is NOT a separate table: dead jobs stay in `jobs` with
     *    state='dead'. A separate table would force a cross-table move
     *    (two statements = a transaction to get right) and lose history.
     *    "The DLQ" is therefore a *view* of the same table, which is simpler
     *    and still satisfies the spec's behaviour exactly.
     */
    private static void ensureSchema(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                  id             TEXT PRIMARY KEY,
                  command        TEXT NOT NULL,
                  state          TEXT NOT NULL DEFAULT 'pending'
                                 CHECK (state IN ('pending','processing','completed','failed','dead')),
                  attempts       INTEGER NOT NULL DEFAULT 0,
                  max_retries    INTEGER NOT NULL DEFAULT 3,
                  priority       INTEGER NOT NULL DEFAULT 0,
                  created_at     TEXT NOT NULL,
                  updated_at     TEXT NOT NULL,
                  next_run_at    TEXT,          -- null = runnable now; else earliest eligible time
                  claimed_by     TEXT,          -- worker id currently/last processing this job
                  last_exit_code INTEGER,
                  last_output    TEXT
                )""");
            // The claim query filters on (state, next_run_at) and orders by (priority, created_at).
            st.execute("CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(state, next_run_at, priority, created_at)");

            st.execute("""
                CREATE TABLE IF NOT EXISTS config (
                  key   TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )""");

            // Worker registry: powers `status` (active workers) and `worker stop`.
            st.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                  id             TEXT PRIMARY KEY,
                  pid            INTEGER,
                  started_at     TEXT NOT NULL,
                  last_heartbeat TEXT NOT NULL,
                  current_job    TEXT
                )""");

            // Single-row control flags. `worker stop` is a CLI process telling
            // *other* processes to wind down — a shared DB flag is the simplest
            // reliable cross-process signal (no OS-specific signal handling).
            st.execute("""
                CREATE TABLE IF NOT EXISTS control (
                  key   TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )""");
        }
    }
}
