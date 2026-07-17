package com.flam.queuectl.model;

import com.flam.queuectl.util.Json;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single background job. Mirrors the JSON spec in the assignment:
 *
 * {
 *   "id": "unique-job-id",
 *   "command": "echo 'Hello World'",
 *   "state": "pending",
 *   "attempts": 0,
 *   "max_retries": 3,
 *   "created_at": "...", "updated_at": "..."
 * }
 *
 * WHY a mutable POJO instead of a record: workers mutate state/attempts as a
 * job moves through its lifecycle, and the storage layer re-hydrates jobs
 * from SQLite rows. A simple mutable class keeps that flow obvious. (In a
 * larger system I'd make this immutable and return new copies, but for a
 * CLI tool the simplicity wins.)
 *
 * Extra fields beyond the spec (all justified bonuses):
 *  - nextRunAt: when a failed job becomes eligible again (exponential backoff)
 *               and also powers the "scheduled jobs (run_at)" bonus feature.
 *  - priority:  higher runs first — "job priority queues" bonus.
 *  - claimedBy: which worker atomically claimed the job (debuggability +
 *               part of the duplicate-processing defence).
 *  - lastExitCode / lastOutput: "job output logging" bonus, and invaluable
 *               when explaining WHY a job died in the DLQ.
 */
public class Job {

    private String id;
    private String command;
    private JobState state = JobState.PENDING;
    private int attempts = 0;
    private int maxRetries;
    private int priority = 0;
    private String createdAt;
    private String updatedAt;
    private String nextRunAt;    // nullable: null = eligible immediately
    private String claimedBy;    // nullable
    private Integer lastExitCode; // nullable
    private String lastOutput;   // nullable

    public Job(String id, String command, int maxRetries) {
        this.id = id;
        this.command = command;
        this.maxRetries = maxRetries;
        String now = Instant.now().toString();
        this.createdAt = now;
        this.updatedAt = now;
    }

    /** Used by the storage layer when re-hydrating from a DB row. */
    public Job() { }

    // --- getters / setters (kept boring on purpose) ---

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getCommand() { return command; }
    public void setCommand(String command) { this.command = command; }

    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public String getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }

    public String getNextRunAt() { return nextRunAt; }
    public void setNextRunAt(String nextRunAt) { this.nextRunAt = nextRunAt; }

    public String getClaimedBy() { return claimedBy; }
    public void setClaimedBy(String claimedBy) { this.claimedBy = claimedBy; }

    public Integer getLastExitCode() { return lastExitCode; }
    public void setLastExitCode(Integer lastExitCode) { this.lastExitCode = lastExitCode; }

    public String getLastOutput() { return lastOutput; }
    public void setLastOutput(String lastOutput) { this.lastOutput = lastOutput; }

    /**
     * Serialise for `list` / `dlq list` output. Uses a LinkedHashMap so the
     * printed field order matches the spec (nicer for humans and demos).
     */
    public String toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("command", command);
        m.put("state", state.dbValue());
        m.put("attempts", attempts);
        m.put("max_retries", maxRetries);
        m.put("priority", priority);
        m.put("created_at", createdAt);
        m.put("updated_at", updatedAt);
        if (nextRunAt != null) m.put("next_run_at", nextRunAt);
        if (claimedBy != null) m.put("claimed_by", claimedBy);
        if (lastExitCode != null) m.put("last_exit_code", lastExitCode);
        if (lastOutput != null && !lastOutput.isBlank()) m.put("last_output", lastOutput.strip());
        return Json.write(m);
    }
}
