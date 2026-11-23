package org.ultrastonk.eventemitter;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplayPacerTest {

    /** Virtual nanosecond clock: sleeping advances time without blocking the test. */
    static final class ManualClock implements ReplayPacer.NanoSource {
        long nanos;
        final List<Long> sleeps = new ArrayList<>();

        @Override
        public long nanoTime() {
            return nanos;
        }

        @Override
        public void sleepNanos(long nanos) {
            sleeps.add(nanos);
            this.nanos += nanos;
        }

        void advanceMillis(long millis) {
            nanos += millis * 1_000_000L;
        }
    }

    private static ReplayPacer capturePacer(ManualClock clock, long prerollMs, long measureMs) {
        return new ReplayPacer(ReplayPacer.Mode.CAPTURE, ReplayPacer.MissingCaptureTime.EVENT_TIME,
                prerollMs, measureMs, 1.0, 0, clock);
    }

    @Test
    void captureClockDrivesSleepsAndEventClockIsIgnored() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 0, 0);

        pacer.pace(1_000L, 50_000L);
        pacer.pace(1_500L, 50_000L);

        assertEquals(List.of(500_000_000L), clock.sleeps, "a 500 ms capture gap must be paced even when event time is tied");
    }

    @Test
    void eventGapsDoNotPaceWhenCaptureTimesAreTied() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 0, 0);

        pacer.pace(1_000L, 1_000L);
        pacer.pace(1_000L, 11_000L);

        assertEquals(List.of(), clock.sleeps, "a 10 s exchange event gap must not delay a same-capture-time row");
    }

    @Test
    void preservesRecordedOrderAndGapsAtASpeedMultiplier() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = new ReplayPacer(ReplayPacer.Mode.CAPTURE, ReplayPacer.MissingCaptureTime.EVENT_TIME,
                0, 0, 2.0, 0, clock);
        List<Long> order = new ArrayList<>();

        for (long capture : new long[]{0L, 100L, 1_000L}) {
            order.add(pacer.pace(capture, capture).timeMs());
        }

        assertEquals(List.of(0L, 100L, 1_000L), order, "rows must be emitted in recorded capture order");
        assertEquals(List.of(50_000_000L, 450_000_000L), clock.sleeps, "gaps are compressed by the speed factor");
    }

    @Test
    void outOfOrderCaptureTimesAreEmittedImmediately() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 0, 0);

        pacer.pace(1_000L, 1_000L);
        ReplayPacer.Pace second = pacer.pace(900L, 900L);

        assertTrue(second.outOfOrder());
        assertEquals(0L, second.scheduledOffsetMs());
        assertEquals(List.of(), clock.sleeps);
    }

    @Test
    void longGapsAreCappedAndTheScheduleIsRebased() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = new ReplayPacer(ReplayPacer.Mode.CAPTURE, ReplayPacer.MissingCaptureTime.EVENT_TIME,
                0, 0, 1.0, 100, clock);

        pacer.pace(0L, 0L);
        ReplayPacer.Pace capped = pacer.pace(5_000L, 5_000L);
        pacer.pace(5_001L, 5_001L);

        assertTrue(capped.capped());
        assertEquals(List.of(100_000_000L, 1_000_000L), clock.sleeps,
                "the post-gap row must pace from the capped point, not the skipped gap");
    }

    @Test
    void separatesPrerollMeasuredAndTrailingInput() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 50, 100);

        assertEquals(ReplayPacer.Segment.PRE_ROLL, pacer.pace(1_000L, 1_000L).segment());
        assertEquals(ReplayPacer.Segment.PRE_ROLL, pacer.pace(1_040L, 1_040L).segment());
        assertEquals(ReplayPacer.Segment.MEASURED, pacer.pace(1_100L, 1_100L).segment());
        assertEquals(ReplayPacer.Segment.TRAILING, pacer.pace(1_200L, 1_200L).segment());

        JsonNode report = pacer.report();
        assertEquals(2, report.at("/segments/PRE_ROLL/rows").asInt());
        assertEquals(1, report.at("/segments/MEASURED/rows").asInt());
        assertEquals(1, report.at("/segments/TRAILING/rows").asInt());
        assertEquals(50, report.at("/preroll_ms").asInt());
        assertEquals(100, report.at("/measure_ms").asInt());
    }

    @Test
    void zeroMeasureDurationLeavesEverythingAfterPrerollAsMeasured() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 10, 0);

        assertEquals(ReplayPacer.Segment.PRE_ROLL, pacer.pace(1_000L, 1_000L).segment());
        assertEquals(ReplayPacer.Segment.MEASURED, pacer.pace(2_000L, 2_000L).segment());
        assertEquals(ReplayPacer.Segment.MEASURED, pacer.pace(99_000L, 99_000L).segment());
    }

    @Test
    void missingCaptureTimeFallsBackToEventTimeByDefault() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = new ReplayPacer(ReplayPacer.Mode.CAPTURE, ReplayPacer.MissingCaptureTime.EVENT_TIME,
                0, 0, 1.0, 0, clock);

        ReplayPacer.Pace first = pacer.pace(null, 10_000L);
        pacer.pace(null, 10_500L);

        assertEquals(10_000L, first.timeMs());
        assertEquals(List.of(500_000_000L), clock.sleeps);
        assertEquals(2, pacer.report().at("/missing_capture_rows").asInt());
    }

    @Test
    void missingCaptureTimeCanBeHeldWithoutInventingAGap() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = new ReplayPacer(ReplayPacer.Mode.CAPTURE, ReplayPacer.MissingCaptureTime.NO_WAIT,
                0, 0, 1.0, 0, clock);

        pacer.pace(1_000L, 1_000L);
        ReplayPacer.Pace held = pacer.pace(null, 9_000L);

        assertEquals(1_000L, held.timeMs());
        assertEquals(List.of(), clock.sleeps);
    }

    @Test
    void missingCaptureTimeCanFailTheMeasuredRun() {
        ReplayPacer pacer = new ReplayPacer(ReplayPacer.Mode.CAPTURE, ReplayPacer.MissingCaptureTime.FAIL,
                0, 0, 1.0, 0, new ManualClock());

        assertThrows(IllegalStateException.class, () -> pacer.pace(null, 1_000L));
    }

    @Test
    void scheduledVersusActualPacingExposesAPublisherThatFallsBehind() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 0, 0);

        pacer.pace(0L, 0L);
        clock.advanceMillis(2_500); // a slow send consumes the whole scheduled gap and more
        pacer.pace(1_000L, 1_000L);
        pacer.pace(2_000L, 2_000L);

        JsonNode measured = pacer.report().at("/segments/MEASURED");
        assertEquals(2_000, measured.get("scheduled_ms").asLong());
        assertEquals(2_500, measured.get("actual_ms").asLong());
        assertEquals(1_500, measured.get("max_lag_ms").asLong());
        assertTrue(measured.get("achieved_ratio").asDouble() < 0.98,
                "falling behind must be visible as an achieved ratio below the tolerance");
    }

    @Test
    void eventPacingKeepsTheExistingScheduleWhenCaptureTimesArePresent() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = new ReplayPacer(ReplayPacer.Mode.EVENT, ReplayPacer.MissingCaptureTime.EVENT_TIME,
                0, 0, 1.0, 0, clock);

        pacer.pace(9_000L, 1_000L);
        pacer.pace(9_000L, 1_500L);

        assertEquals(List.of(500_000_000L), clock.sleeps, "event mode still paces by exchange event time");
        assertEquals(1_000L, pacer.report().at("/segments/MEASURED/first_time_ms").asLong());
    }

    @Test
    void reportsClockCorrelationBetweenCaptureAndEventTime() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 0, 0);

        pacer.pace(100_000L, 10_000L);
        pacer.pace(101_000L, 11_500L);

        JsonNode offset = pacer.report().get("clock_offset");
        assertEquals(2, offset.get("samples").asInt());
        assertEquals(89_500, offset.get("min_ms").asLong());
        assertEquals(90_000, offset.get("max_ms").asLong());
        assertFalse(offset.get("p50_ms").isMissingNode());
    }

    @Test
    void reportExposesRunSettingsForReproducibility() throws InterruptedException {
        ManualClock clock = new ManualClock();
        ReplayPacer pacer = capturePacer(clock, 0, 0);
        pacer.pace(1_000L, 1_000L);

        JsonNode report = pacer.report();
        assertEquals("capture", report.get("mode").asText());
        assertEquals("event", report.get("missing_capture_time_policy").asText());
        assertEquals(1.0, report.get("speed_multiplier").asDouble());
        assertTrue(report.get("samples").isArray());
        assertNull(report.get("segments").get("TRAILING").get("first_time_ms").textValue());
    }
}
