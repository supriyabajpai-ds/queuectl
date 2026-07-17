package com.flam.queuectl.cli;

import com.flam.queuectl.model.Job;
import com.flam.queuectl.model.JobState;
import com.flam.queuectl.storage.ConfigStore;
import com.flam.queuectl.storage.JobStore;
import com.flam.queuectl.storage.WorkerRegistry;
import com.flam.queuectl.util.Json;
import com.flam.queuectl.worker.WorkerPool;

import java.util.List;
import java.util.Map;

/**
 * CLI entry point. Parses argv by hand and dispatches to the layers below.
 *
 * WHY manual parsing instead of picocli/JCommander: the command surface is
 * small and fixed (7 commands), and hand-parsing keeps the tool at zero
 * runtime dependencies beyond the JDBC driver. The trade-off is writing our
 * own help text — which the spec wants anyway ("clean CLI interface,
 * commands & help texts"). For a bigger CLI I'd reach for picocli.
 *
 * Every user-facing error exits with a non-zero status and a message on
 * stderr; success output goes to stdout. That separation makes the tool
 * scriptable (test.sh depends on it).
 */
public final class Main {

    public static void main(String[] args) {
        try {
            int code = dispatch(args);
            System.exit(code);
        } catch (IllegalArgumentException | Json.JsonException e) {
            // Expected user errors: bad input, unknown key, duplicate id...
            System.err.println("error: " + e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            // Unexpected: show the cause but never a naked stack trace wall.
            System.err.println("unexpected error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static int dispatch(String[] args) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help") || args[0].equals("-h")) {
            printHelp();
            return 0;
        }
        return switch (args[0]) {
            case "enqueue" -> cmdEnqueue(args);
            case "worker" -> cmdWorker(args);
            case "status" -> cmdStatus();
            case "list" -> cmdList(args);
            case "dlq" -> cmdDlq(args);
            case "config" -> cmdConfig(args);
            default -> {
                System.err.println("error: unknown command '" + args[0] + "'\n");
                printHelp();
                yield 2;
            }
        };
    }

    // ---------------- enqueue ----------------

    private static int cmdEnqueue(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: queuectl enqueue '<job-json>'");
        }
        Map<String, Object> spec = Json.parseObject(args[1]);

        String id = requireString(spec, "id");
        String command = requireString(spec, "command");
        if (id.isBlank()) throw new IllegalArgumentException("job 'id' must not be blank");
        if (command.isBlank()) throw new IllegalArgumentException("job 'command' must not be blank");

        // max_retries can be supplied per job; otherwise the CURRENT config
        // default is snapshotted onto the job at enqueue time. Snapshotting
        // (vs reading config at run time) means changing the global default
        // later doesn't retroactively change the contract of jobs already
        // in the queue.
        int maxRetries = spec.containsKey("max_retries")
                ? toInt(spec.get("max_retries"), "max_retries")
                : ConfigStore.getInt(ConfigStore.MAX_RETRIES);
        if (maxRetries < 1) throw new IllegalArgumentException("max_retries must be >= 1");

        Job job = new Job(id, command, maxRetries);

        if (spec.containsKey("priority")) {
            job.setPriority(toInt(spec.get("priority"), "priority"));
        }
        // Bonus: scheduled jobs. run_at is an ISO-8601 instant; the job stays
        // pending but the claim query skips it until the time arrives.
        if (spec.containsKey("run_at")) {
            String runAt = String.valueOf(spec.get("run_at"));
            try {
                java.time.Instant.parse(runAt);
            } catch (Exception e) {
                throw new IllegalArgumentException("run_at must be an ISO-8601 instant like 2026-07-17T10:30:00Z");
            }
            job.setNextRunAt(runAt);
        }

        JobStore.enqueue(job);
        System.out.println("enqueued job '" + id + "' (max_retries=" + maxRetries
                + (job.getNextRunAt() != null ? ", scheduled for " + job.getNextRunAt() : "") + ")");
        return 0;
    }

    // ---------------- worker ----------------

    private static int cmdWorker(String[] args) {
        if (args.length < 2) throw new IllegalArgumentException("usage: queuectl worker <start|stop> [--count N]");
        switch (args[1]) {
            case "start" -> {
                int count = 1;
                for (int i = 2; i < args.length; i++) {
                    if (args[i].equals("--count") && i + 1 < args.length) {
                        count = toInt(args[++i], "--count");
                    }
                }
                if (count < 1) throw new IllegalArgumentException("--count must be >= 1");
                new WorkerPool(count).startAndBlock(); // blocks until stopped
                return 0;
            }
            case "stop" -> {
                WorkerRegistry.requestStop();
                int active = WorkerRegistry.activeWorkers().size();
                System.out.println("stop requested. " + active + " active worker(s) will finish their current job and exit.");
                return 0;
            }
            default -> throw new IllegalArgumentException("unknown worker subcommand '" + args[1] + "'");
        }
    }

    // ---------------- status ----------------

    private static int cmdStatus() {
        Map<String, Integer> counts = JobStore.countsByState();
        List<WorkerRegistry.WorkerInfo> workers = WorkerRegistry.activeWorkers();

        System.out.println("Jobs:");
        counts.forEach((state, n) -> System.out.printf("  %-11s %d%n", state, n));
        System.out.println("Active workers: " + workers.size());
        for (WorkerRegistry.WorkerInfo w : workers) {
            System.out.printf("  %-28s pid=%-8d job=%s%n",
                    w.id(), w.pid(), w.currentJob() == null ? "-" : w.currentJob());
        }
        return 0;
    }

    // ---------------- list ----------------

    private static int cmdList(String[] args) {
        JobState filter = null;
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("--state") && i + 1 < args.length) {
                try {
                    filter = JobState.fromDb(args[++i]);
                } catch (Exception e) {
                    throw new IllegalArgumentException("unknown state '" + args[i]
                            + "'. Valid: pending, processing, completed, failed, dead");
                }
            }
        }
        List<Job> jobs = JobStore.list(filter);
        if (jobs.isEmpty()) {
            System.out.println("no jobs" + (filter != null ? " in state '" + filter.dbValue() + "'" : ""));
            return 0;
        }
        for (Job j : jobs) System.out.println(j.toJson());
        return 0;
    }

    // ---------------- dlq ----------------

    private static int cmdDlq(String[] args) {
        if (args.length < 2) throw new IllegalArgumentException("usage: queuectl dlq <list|retry <job-id>>");
        switch (args[1]) {
            case "list" -> {
                List<Job> dead = JobStore.list(JobState.DEAD);
                if (dead.isEmpty()) {
                    System.out.println("DLQ is empty");
                } else {
                    for (Job j : dead) System.out.println(j.toJson());
                }
                return 0;
            }
            case "retry" -> {
                if (args.length < 3) throw new IllegalArgumentException("usage: queuectl dlq retry <job-id>");
                String id = args[2];
                if (JobStore.retryFromDlq(id)) {
                    System.out.println("job '" + id + "' moved from DLQ back to pending (attempts reset)");
                    return 0;
                }
                // Distinguish "no such job" from "job exists but isn't dead"
                // — precise errors are half of a good CLI.
                if (JobStore.get(id).isPresent()) {
                    throw new IllegalArgumentException("job '" + id + "' exists but is not in the DLQ");
                }
                throw new IllegalArgumentException("no job with id '" + id + "'");
            }
            default -> throw new IllegalArgumentException("unknown dlq subcommand '" + args[1] + "'");
        }
    }

    // ---------------- config ----------------

    private static int cmdConfig(String[] args) {
        if (args.length < 2) throw new IllegalArgumentException("usage: queuectl config <set <key> <value>|get <key>|list>");
        switch (args[1]) {
            case "set" -> {
                if (args.length < 4) throw new IllegalArgumentException("usage: queuectl config set <key> <value>");
                ConfigStore.set(args[2], args[3]);
                System.out.println(args[2] + " = " + args[3]);
                return 0;
            }
            case "get" -> {
                if (args.length < 3) throw new IllegalArgumentException("usage: queuectl config get <key>");
                System.out.println(ConfigStore.get(args[2]));
                return 0;
            }
            case "list" -> {
                ConfigStore.all().forEach((k, v) -> System.out.println(k + " = " + v));
                return 0;
            }
            default -> throw new IllegalArgumentException("unknown config subcommand '" + args[1] + "'");
        }
    }

    // ---------------- helpers ----------------

    private static String requireString(Map<String, Object> spec, String key) {
        Object v = spec.get(key);
        if (v == null) throw new IllegalArgumentException("job JSON is missing required field '" + key + "'");
        return String.valueOf(v);
    }

    private static int toInt(Object v, String field) {
        try {
            if (v instanceof Number n) return n.intValue();
            return Integer.parseInt(String.valueOf(v));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + " must be an integer, got '" + v + "'");
        }
    }

    private static void printHelp() {
        System.out.println("""
            queuectl — CLI background job queue with retries, backoff and a DLQ

            Usage:
              queuectl enqueue '<job-json>'          Add a job. JSON needs "id" and "command";
                                                     optional: "max_retries", "priority", "run_at".
              queuectl worker start --count N        Start N workers (blocks; Ctrl+C or `worker stop`).
              queuectl worker stop                   Ask all workers to finish current job and exit.
              queuectl status                        Job counts by state + active workers.
              queuectl list [--state <state>]        List jobs (pending|processing|completed|failed|dead).
              queuectl dlq list                      Show permanently failed (dead) jobs.
              queuectl dlq retry <job-id>            Move a DLQ job back to pending.
              queuectl config set <key> <value>      Keys: max-retries, backoff-base,
                                                     poll-interval-ms, job-timeout-seconds.
              queuectl config get <key> | config list
              queuectl help                          Show this help.

            Examples:
              queuectl enqueue '{"id":"job1","command":"echo hello"}'
              queuectl worker start --count 3
              queuectl config set backoff-base 2
            """);
    }

    private Main() { }
}
