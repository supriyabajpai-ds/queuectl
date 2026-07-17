package com.flam.queuectl.worker;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Executes a job's shell command via ProcessBuilder and reports the result.
 *
 * Design decisions:
 *  - Commands run THROUGH a shell (`sh -c` on Unix, `cmd /c` on Windows)
 *    rather than being tokenised by us. The spec's examples ("echo 'Hello
 *    World'", "sleep 2") assume shell semantics (quoting, operators like
 *    && and pipes), and hand-tokenising shell syntax correctly is a rabbit
 *    hole. OS detection keeps this working on Windows dev machines and
 *    Linux CI alike.
 *  - Exit code is the ONLY success signal (0 = success), exactly as the
 *    spec requires. A command that doesn't exist makes the shell itself
 *    exit non-zero (127 on sh, 1 on cmd), so "command not found" flows
 *    through the same failure→retry path with no special-casing.
 *  - stderr is merged into stdout (redirectErrorStream) so the DLQ's
 *    last_output shows the actual error message — when a job dies, the
 *    first debugging question is always "what did it print?".
 *  - Optional timeout (config job-timeout-seconds, 0 = off): a hung job
 *    would otherwise wedge its worker forever. On timeout we destroy the
 *    process tree and report a synthetic exit code 124 (the same convention
 *    the GNU `timeout` utility uses), which then rides the normal retry path.
 */
public final class CommandRunner {

    public static final int TIMEOUT_EXIT_CODE = 124;
    private static final int MAX_CAPTURED_OUTPUT_CHARS = 8_000;

    public record Result(int exitCode, String output, boolean timedOut) {
        public boolean success() { return exitCode == 0; }
    }

    public static Result run(String command, long timeoutSeconds) {
        ProcessBuilder pb = isWindows()
                ? new ProcessBuilder("cmd.exe", "/c", command)
                : new ProcessBuilder("sh", "-c", command);
        pb.redirectErrorStream(true);

        try {
            Process process = pb.start();

            // Read output on this thread while waiting. Draining the stream
            // matters: if the pipe buffer fills and nobody reads it, the
            // child blocks on write and the job "hangs" for no visible reason.
            StringBuilder out = new StringBuilder();
            Thread reader = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (out.length() < MAX_CAPTURED_OUTPUT_CHARS) {
                            out.append(line).append('\n');
                        }
                    }
                } catch (Exception ignored) {
                    // Stream closes when the process dies; nothing to do.
                }
            });
            reader.start();

            boolean finished;
            if (timeoutSeconds > 0) {
                finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            } else {
                process.waitFor();
                finished = true;
            }

            if (!finished) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
                reader.join(1000);
                return new Result(TIMEOUT_EXIT_CODE,
                        out + "\n[queuectl] job killed after exceeding timeout of " + timeoutSeconds + "s",
                        true);
            }

            reader.join(2000);
            return new Result(process.exitValue(), out.toString(), false);

        } catch (Exception e) {
            // e.g. the shell itself could not be started. Treat as a normal
            // failure so it enters the retry path instead of crashing the worker.
            return new Result(-1, "[queuectl] failed to launch command: " + e.getMessage(), false);
        }
    }

    static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    private CommandRunner() { }
}
