package com.flam.queuectl.model;

/**
 * The five lifecycle states from the assignment spec.
 *
 * WHY an enum and not raw strings: the state machine is the heart of this
 * system. An enum makes illegal states unrepresentable in Java code — a typo
 * like "pendng" becomes a compile error instead of a silent bug where a job
 * is never picked up. We only convert to/from lowercase strings at the
 * database boundary.
 *
 * Lifecycle:
 *   pending → processing → completed
 *                        → failed → (backoff delay) → processing → ...
 *                        → dead (after max_retries exhausted; lives in DLQ)
 */
public enum JobState {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    DEAD;

    /** DB / JSON representation ("pending", "processing", ...). */
    public String dbValue() {
        return name().toLowerCase();
    }

    public static JobState fromDb(String value) {
        return JobState.valueOf(value.trim().toUpperCase());
    }
}
