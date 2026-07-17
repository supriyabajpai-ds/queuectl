package com.flam.queuectl.worker;

import com.flam.queuectl.model.Job;
import com.flam.queuectl.model.JobState;
import com.flam.queuectl.storage.ConfigStore;
import com.flam.queuectl.storage.JobStore;
import com.flam.queuectl.storage.WorkerRegistry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Runs N workers inside this process and blocks until they stop.
 *
 * WHY threads-in-one-process (each running the same claim protocol) instead
 * of forking N OS processes: the duplicate-processing guarantee lives in the
 * DATABASE (JobStore.claimNext), not in shared Java memory — so the design
 * is process-agnostic. You can even run `worker start` in three separate
 * terminals and the workers still never collide, which is easy to demo.
 * Given that, one process with N threads is operationally simpler: one thing
 * to start, one console for logs, one Ctrl+C to stop. If true process
 * isolation were needed (a job that OOMs the JVM), the claim protocol
 * wouldn't change at all — only the launcher would.
 *
 * Graceful shutdown ("finish current job before exit") has two triggers:
 *   1. `queuectl worker stop` from another terminal → sets the DB stop flag.
 *   2. Ctrl+C on this process → JVM shutdown hook flips a volatile flag.
 * Both are checked ONLY between jobs, at the top of the loop. A worker
 * mid-execution finishes its command, records the result, and only then
 * notices the flag — so no job is ever half-done because of a stop.
 */
public final class WorkerPool {

    /**
     * volatile so a write from the shutdown-hook thread is immediately
     * visible to every worker thread — without it, the JMM allows workers
     * to keep reading a stale 'false' from their own cache indefinitely.
     */
    private volatile boolean localStop = false;

    private final int count;
    private final List<Thread> threads = new ArrayList<>();

    public WorkerPool(int count) {
        this.count = count;
    }

    public void startAndBlock() {
        long pid = ProcessHandle.current().pid();

        // A fresh start means: previous stop requests are obsolete, worker
        // rows from crashed runs are stale, and any 'processing' jobs are
        // orphans from a crash (no workers are alive right now) → recover
        // them so they run again instead of being lost.
        WorkerRegistry.clearStop();
        WorkerRegistry.pruneStale();
        int recovered = JobStore.recoverOrphans();
        if (recovered > 0) {
            log("recovered " + recovered + " orphaned processing job(s) back to pending");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            localStop = true;
            for (Thread t : threads) {
                try { t.join(30_000); } catch (InterruptedException ignored) { }
            }
        }));

        for (int i = 1; i <= count; i++) {
            // Short random suffix keeps ids unique even across multiple
            // `worker start` invocations running at the same time.
            String workerId = "worker-" + pid + "-" + i + "-"
                    + UUID.randomUUID().toString().substring(0, 4);
            Thread t = new Thread(() -> workerLoop(workerId), workerId);
            threads.add(t);
            t.start();
        }
        log("started " + count + " worker(s), pid " + pid + ". Press Ctrl+C or run 'queuectl worker stop' to stop.");

        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException ignored) { }
        }
        log("all workers stopped.");
    }

    private void workerLoop(String workerId) {
        WorkerRegistry.register(workerId, ProcessHandle.current().pid());
        log(workerId + " online");
        try {
            while (true) {
                // Stop checks happen HERE, between jobs — the definition of
                // graceful: current job always completes first.
                if (localStop || WorkerRegistry.stopRequested()) break;

                Optional<Job> claimed = JobStore.claimNext(workerId);
                if (claimed.isEmpty()) {
                    // Nothing runnable: heartbeat (so `status` sees us alive)
                    // and back off for poll-interval-ms before looking again.
                    WorkerRegistry.heartbeat(workerId, null);
                    sleep(ConfigStore.getInt(ConfigStore.POLL_INTERVAL_MS));
                    continue;
                }

                Job job = claimed.get();
                WorkerRegistry.heartbeat(workerId, job.getId());
                execute(workerId, job);
            }
        } finally {
            WorkerRegistry.deregister(workerId);
            log(workerId + " stopped gracefully");
        }
    }

    private void execute(String workerId, Job job) {
        log(workerId + " -> picked job '" + job.getId() + "' (attempt " + (job.getAttempts() + 1)
                + "/" + job.getMaxRetries() + "): " + job.getCommand());

        long timeout = ConfigStore.getInt(ConfigStore.JOB_TIMEOUT_SECONDS);
        CommandRunner.Result result = CommandRunner.run(job.getCommand(), timeout);

        if (result.success()) {
            JobStore.markCompleted(job.getId(), result.exitCode(), result.output());
            log(workerId + " [OK] job '" + job.getId() + "' completed");
            return;
        }

        // Backoff base is read at failure time (not enqueue time) so a
        // `config set backoff-base` change applies to in-flight retries too.
        int backoffBase = ConfigStore.getInt(ConfigStore.BACKOFF_BASE);
        JobState outcome = JobStore.markFailed(job, result.exitCode(), result.output(), backoffBase);

        if (outcome == JobState.DEAD) {
            log(workerId + " [FAIL] job '" + job.getId() + "' exhausted " + job.getMaxRetries()
                    + " attempts (exit " + result.exitCode() + ") -> moved to DLQ");
        } else {
            long delay = (long) Math.pow(backoffBase, job.getAttempts() + 1);
            log(workerId + " [FAIL] job '" + job.getId() + "' failed (exit " + result.exitCode()
                    + ") -> retry in " + delay + "s");
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void log(String msg) {
        System.out.println("[" + Instant.now() + "] " + msg);
    }
}
