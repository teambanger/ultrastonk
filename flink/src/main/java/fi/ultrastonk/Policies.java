package fi.ultrastonk;

import java.io.Serializable;
import java.util.Locale;

public final class Policies implements Serializable {
    public enum Deduplication { NONE, EXACT, TIMESTAMP }
    public enum LateEvents { DROP, SIDE_OUTPUT, GRACE }

    public final Deduplication deduplication;
    public final LateEvents lateEvents;
    public final long dedupTtlMs;
    public final long graceMs;
    public final long logIntervalMs;

    public Policies(Deduplication deduplication, LateEvents lateEvents, long dedupTtlMs, long graceMs, long logIntervalMs) {
        if (dedupTtlMs <= 0 || graceMs < 0 || graceMs > 86_400_000 || logIntervalMs <= 0) {
            throw new IllegalArgumentException("TTL/log interval must be positive; grace must be in [0, 86400000]");
        }
        this.deduplication = deduplication;
        this.lateEvents = lateEvents;
        this.dedupTtlMs = dedupTtlMs;
        this.graceMs = graceMs;
        this.logIntervalMs = logIntervalMs;
    }

    public static Policies defaults() {
        return new Policies(Deduplication.NONE, LateEvents.DROP, 3600000, 30000, 10000);
    }

    public static Policies fromEnvironment() {
        return new Policies(Deduplication.valueOf(env("DEDUP_STRATEGY", "NONE").toUpperCase(Locale.ROOT)),
                LateEvents.valueOf(env("LATE_EVENT_STRATEGY", "DROP").toUpperCase(Locale.ROOT)),
                Long.parseLong(env("DEDUP_TTL_MS", "3600000")),
                Long.parseLong(env("ALLOWED_LATENESS_MS", "30000")),
                Long.parseLong(env("LOG_INTERVAL_MS", "10000")));
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
