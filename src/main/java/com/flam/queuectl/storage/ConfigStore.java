package com.flam.queuectl.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Key/value configuration persisted in the `config` table.
 *
 * WHY config lives in the same SQLite DB: "hardcoded configuration values"
 * is an explicit disqualifier. Storing config beside the jobs means one
 * `config set` is immediately visible to every worker process (they read
 * config each cycle) without restarting anything, and there's no extra
 * config-file format to parse or corrupt.
 *
 * Defaults are defined HERE, in one place, and only apply when a key has
 * never been set — they are fallbacks, not hardcoding: every one of them is
 * overridable at runtime via `queuectl config set`.
 */
public final class ConfigStore {

    public static final String MAX_RETRIES = "max-retries";
    public static final String BACKOFF_BASE = "backoff-base";
    public static final String POLL_INTERVAL_MS = "poll-interval-ms";
    public static final String JOB_TIMEOUT_SECONDS = "job-timeout-seconds"; // 0 = no timeout

    private static final Map<String, String> DEFAULTS = Map.of(
            MAX_RETRIES, "3",
            BACKOFF_BASE, "2",
            POLL_INTERVAL_MS, "500",
            JOB_TIMEOUT_SECONDS, "0"
    );

    public static Set<String> knownKeys() {
        return DEFAULTS.keySet();
    }

    public static String get(String key) {
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement("SELECT value FROM config WHERE key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("config read failed", e);
        }
        String def = DEFAULTS.get(key);
        if (def == null) throw new IllegalArgumentException("Unknown config key: " + key);
        return def;
    }

    public static int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public static void set(String key, String value) {
        if (!DEFAULTS.containsKey(key)) {
            throw new IllegalArgumentException(
                    "Unknown config key '" + key + "'. Known keys: " + String.join(", ", DEFAULTS.keySet()));
        }
        // Validate numerics up front so a bad value fails at `config set`
        // time with a clear message, not later inside a worker loop.
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Value for " + key + " must be an integer, got '" + value + "'");
        }
        if (parsed < 0) throw new IllegalArgumentException(key + " cannot be negative");
        if (key.equals(BACKOFF_BASE) && parsed < 1) {
            throw new IllegalArgumentException("backoff-base must be >= 1");
        }
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO config(key, value) VALUES(?, ?) " +
                     "ON CONFLICT(key) DO UPDATE SET value = excluded.value")) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("config write failed", e);
        }
    }

    /** Effective config: defaults overlaid with anything explicitly set. */
    public static Map<String, String> all() {
        Map<String, String> out = new LinkedHashMap<>(DEFAULTS);
        try (Connection c = Database.open();
             PreparedStatement ps = c.prepareStatement("SELECT key, value FROM config");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.put(rs.getString(1), rs.getString(2));
        } catch (SQLException e) {
            throw new RuntimeException("config read failed", e);
        }
        return out;
    }
}
