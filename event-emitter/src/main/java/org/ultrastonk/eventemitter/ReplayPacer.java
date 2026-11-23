package org.ultrastonk.eventemitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Drift-free replay pacing over either the exchange event clock or the recorded capture clock.
 *
 * <p>{@link Mode#EVENT} is the historical default: rows sleep until wall-clock time reaches their
 * exchange event time, so query event time and pacing time are the same clock. {@link Mode#CAPTURE}
 * instead paces by the CSV system date/time columns while the payload keeps the exchange event
 * time, so a replay can reproduce the recorded publication gaps of a live capture without changing
 * event-time semantics downstream.
 *
 * <p>Every record is classified as pre-roll, measured or trailing input from its capture offset;
 * the report exposes scheduled versus achieved pacing, the missing-capture-time policy outcome and
 * the observed capture-versus-event clock correlation. Long gaps are capped and the schedule is
 * re-based exactly like the original event-time clock, so a capped gap cannot turn the following
 * burst into one capped sleep per row.
 */
public final class ReplayPacer {

    public enum Mode {
        EVENT("event"), CAPTURE("capture");

        private final String id;

        Mode(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }

        public static Mode fromString(String value) {
            String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
            for (Mode mode : values()) {
                if (mode.id.equals(v)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown PACING '" + value + "', expected event or capture");
        }
    }

    /** What to do with a row whose system date/time columns cannot be read. */
    public enum MissingCaptureTime {
        EVENT_TIME("event"), NO_WAIT("no-wait"), FAIL("fail");

        private final String id;

        MissingCaptureTime(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }

        public static MissingCaptureTime fromString(String value) {
            String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
            for (MissingCaptureTime policy : values()) {
                if (policy.id.equals(v)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException("Unknown MISSING_CAPTURE_TIME '" + value
                    + "', expected event, no-wait or fail");
        }
    }

    public enum Segment {
        PRE_ROLL, MEASURED, TRAILING
    }

    /** Injectable time source so pacing schedules can be asserted without real sleeps. */
    public interface NanoSource {
        long nanoTime();

        void sleepNanos(long nanos) throws InterruptedException;

        static NanoSource system() {
            return new NanoSource() {
                @Override
                public long nanoTime() {
                    return System.nanoTime();
                }

                @Override
                public void sleepNanos(long nanos) throws InterruptedException {
                    TimeUnit.NANOSECONDS.sleep(nanos);
                }
            };
        }
    }

    /**
     * One paced record. Offsets are measured from the first paced record of the run;
     * {@code scheduled} is the target and {@code actual} is what the wall clock allowed.
     */
    public record Pace(Segment segment, long timeMs, long scheduledOffsetMs, long actualOffsetMs,
                       boolean first, boolean outOfOrder, boolean capped) {
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long DEFAULT_SAMPLE_LIMIT = 1000;

    private final Mode mode;
    private final MissingCaptureTime missingCaptureTime;
    private final long prerollMs;
    private final long measureMs;
    private final double speedFactor;
    private final long maxSleepMs;
    private final NanoSource clock;

    private long firstTimeMs = -1L;
    private long firstCaptureMs = -1L;
    private long startNano;
    private Long lastTimeMs;
    private Segment lastSegment = Segment.MEASURED;
    private long missingCaptureRows;
    private int sampleLimit = (int) DEFAULT_SAMPLE_LIMIT;
    private final Map<Segment, Stats> stats = new EnumMap<>(Segment.class);
    private final List<ObjectNode> samples = new ArrayList<>();
    private final List<Long> clockOffsetSamples = new ArrayList<>();
    private long clockOffsetCount;
    private long minOffset = Long.MAX_VALUE;
    private long maxOffset = Long.MIN_VALUE;

    public ReplayPacer(Mode mode, MissingCaptureTime missingCaptureTime, long prerollMs, long measureMs,
                       double speedFactor, long maxSleepMs, NanoSource clock) {
        if (!Double.isFinite(speedFactor) || speedFactor <= 0.0) {
            throw new IllegalArgumentException("speed factor must be finite and > 0");
        }
        if (prerollMs < 0 || measureMs < 0 || maxSleepMs < 0) {
            throw new IllegalArgumentException("pacing durations must be nonnegative");
        }
        this.mode = mode == null ? Mode.EVENT : mode;
        this.missingCaptureTime = missingCaptureTime == null ? MissingCaptureTime.EVENT_TIME : missingCaptureTime;
        this.prerollMs = prerollMs;
        this.measureMs = measureMs;
        this.speedFactor = speedFactor;
        this.maxSleepMs = maxSleepMs;
        this.clock = clock == null ? NanoSource.system() : clock;
        for (Segment segment : Segment.values()) {
            stats.put(segment, new Stats());
        }
    }

    public ReplayPacer withSampleLimit(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("sample limit must be nonnegative");
        }
        this.sampleLimit = limit;
        return this;
    }

    public Pace pace(Long captureTimestampMs, long eventTimestampMs) throws InterruptedException {
        long timeMs = pacingTime(captureTimestampMs, eventTimestampMs);
        Segment segment = segmentFor(captureTimestampMs);
        lastSegment = segment;
        if (captureTimestampMs != null) {
            recordClockOffset(captureTimestampMs, eventTimestampMs);
        }

        long now = clock.nanoTime();
        boolean first = firstTimeMs < 0;
        boolean outOfOrder = false;
        long scheduledNanos = 0;
        long actualNanos = 0;
        boolean capped = false;

        if (first) {
            firstTimeMs = timeMs;
            startNano = now;
        } else {
            outOfOrder = timeMs < firstTimeMs;
            long offsetMs = Math.max(0L, timeMs - firstTimeMs);
            scheduledNanos = (long) (offsetMs * 1_000_000.0 / speedFactor);
            long elapsedNanos = now - startNano;
            long sleepNanos = scheduledNanos - elapsedNanos;
            if (sleepNanos > 0) {
                long capNanos = maxSleepMs > 0 ? maxSleepMs * 1_000_000L : Long.MAX_VALUE;
                if (sleepNanos > capNanos) {
                    startNano -= sleepNanos - capNanos;
                    sleepNanos = capNanos;
                    capped = true;
                }
                clock.sleepNanos(sleepNanos);
            }
            actualNanos = clock.nanoTime() - startNano;
        }
        lastTimeMs = timeMs;

        Stats segmentStats = stats.get(segment);
        segmentStats.add(timeMs, scheduledNanos, actualNanos, outOfOrder, capped,
                captureTimestampMs == null);
        if (samples.size() < sampleLimit) {
            ObjectNode sample = MAPPER.createObjectNode();
            sample.put("segment", segment.name());
            sample.put("time_ms", timeMs);
            sample.put("scheduled_offset_ms", scheduledNanos / 1_000_000L);
            sample.put("actual_offset_ms", actualNanos / 1_000_000L);
            if (captureTimestampMs == null) {
                sample.putNull("capture_ms");
            } else {
                sample.put("capture_ms", captureTimestampMs);
            }
            sample.put("event_ms", eventTimestampMs);
            samples.add(sample);
        }

        return new Pace(segment, timeMs, scheduledNanos / 1_000_000L, actualNanos / 1_000_000L,
                first, outOfOrder, capped);
    }

    public long firstTimeMs() {
        return firstTimeMs;
    }

    private long pacingTime(Long captureTimestampMs, long eventTimestampMs) {
        if (mode == Mode.EVENT) {
            return eventTimestampMs;
        }
        if (captureTimestampMs != null) {
            return captureTimestampMs;
        }
        missingCaptureRows++;
        return switch (missingCaptureTime) {
            case EVENT_TIME -> eventTimestampMs;
            case NO_WAIT -> lastTimeMs == null ? eventTimestampMs : lastTimeMs;
            case FAIL -> throw new IllegalStateException(
                    "Capture time is missing for a row and MISSING_CAPTURE_TIME=fail");
        };
    }

    private Segment segmentFor(Long captureTimestampMs) {
        if (mode == Mode.EVENT) {
            return Segment.MEASURED;
        }
        if (captureTimestampMs != null && firstCaptureMs < 0) {
            firstCaptureMs = captureTimestampMs;
        }
        if (captureTimestampMs == null) {
            return lastSegmentOrInitial();
        }
        long offset = captureTimestampMs - firstCaptureMs;
        if (offset < prerollMs) {
            return Segment.PRE_ROLL;
        }
        if (measureMs > 0 && offset >= prerollMs + measureMs) {
            return Segment.TRAILING;
        }
        return Segment.MEASURED;
    }

    private Segment lastSegmentOrInitial() {
        if (lastTimeMs == null) {
            return prerollMs > 0 ? Segment.PRE_ROLL : Segment.MEASURED;
        }
        return lastSegment;
    }

    private void recordClockOffset(long captureTimestampMs, long eventTimestampMs) {
        long offset = captureTimestampMs - eventTimestampMs;
        clockOffsetCount++;
        minOffset = Math.min(minOffset, offset);
        maxOffset = Math.max(maxOffset, offset);
        if (clockOffsetSamples.size() < sampleLimit) {
            clockOffsetSamples.add(offset);
        }
    }

    /** Report fragment; the emitter adds run metadata and enqueue/acknowledgement samples. */
    public ObjectNode report() {
        ObjectNode report = MAPPER.createObjectNode();
        report.put("mode", mode.id());
        report.put("speed_multiplier", speedFactor);
        report.put("missing_capture_time_policy", missingCaptureTime.id());
        report.put("preroll_ms", prerollMs);
        report.put("measure_ms", measureMs);
        report.put("missing_capture_rows", missingCaptureRows);

        ObjectNode segments = report.putObject("segments");
        for (Map.Entry<Segment, Stats> entry : stats.entrySet()) {
            segments.set(entry.getKey().name(), entry.getValue().json());
        }

        ObjectNode offset = report.putObject("clock_offset");
        offset.put("samples", clockOffsetCount);
        if (clockOffsetCount == 0) {
            offset.putNull("min_ms");
            offset.putNull("p50_ms");
            offset.putNull("max_ms");
        } else {
            offset.put("min_ms", minOffset);
            offset.put("p50_ms", percentile(clockOffsetSamples, 0.5));
            offset.put("max_ms", maxOffset);
        }

        ArrayNode sampleArray = report.putArray("samples");
        samples.forEach(sampleArray::add);
        return report;
    }

    private static long percentile(List<Long> values, double p) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compare);
        int index = (int) Math.round(p * (sorted.size() - 1));
        return sorted.get(index);
    }

    private static final class Stats {
        long rows;
        long missingCaptureRows;
        Long firstTimeMs;
        Long lastTimeMs;
        long firstScheduledNanos;
        long lastScheduledNanos;
        long firstActualNanos;
        long lastActualNanos;
        long maxLagNanos;
        long outOfOrderRows;
        long cappedRows;

        void add(long timeMs, long scheduledNanos, long actualNanos, boolean outOfOrder, boolean capped,
                 boolean missingCapture) {
            if (rows == 0) {
                firstTimeMs = timeMs;
                firstScheduledNanos = scheduledNanos;
                firstActualNanos = actualNanos;
            }
            rows++;
            lastTimeMs = timeMs;
            lastScheduledNanos = scheduledNanos;
            lastActualNanos = actualNanos;
            maxLagNanos = Math.max(maxLagNanos, actualNanos - scheduledNanos);
            if (outOfOrder) {
                outOfOrderRows++;
            }
            if (capped) {
                cappedRows++;
            }
            if (missingCapture) {
                missingCaptureRows++;
            }
        }

        ObjectNode json() {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("rows", rows);
            node.put("missing_capture_rows", missingCaptureRows);
            node.put("scheduled_ms", rows == 0 ? 0 : (lastScheduledNanos - firstScheduledNanos) / 1_000_000L);
            node.put("actual_ms", rows == 0 ? 0 : (lastActualNanos - firstActualNanos) / 1_000_000L);
            long scheduledMillis = rows == 0 ? 0 : (lastScheduledNanos - firstScheduledNanos) / 1_000_000L;
            long actualMillis = rows == 0 ? 0 : (lastActualNanos - firstActualNanos) / 1_000_000L;
            if (scheduledMillis <= 0 || actualMillis <= 0) {
                node.putNull("achieved_ratio");
            } else {
                node.put("achieved_ratio", (double) scheduledMillis / (double) actualMillis);
            }
            node.put("max_lag_ms", maxLagNanos / 1_000_000L);
            node.put("out_of_order_rows", outOfOrderRows);
            node.put("capped_rows", cappedRows);
            if (firstTimeMs == null) {
                node.putNull("first_time_ms");
                node.putNull("last_time_ms");
            } else {
                node.put("first_time_ms", firstTimeMs);
                node.put("last_time_ms", lastTimeMs);
            }
            return node;
        }
    }
}
