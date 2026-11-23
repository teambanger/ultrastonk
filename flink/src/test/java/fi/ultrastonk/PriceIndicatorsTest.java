package fi.ultrastonk;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PriceIndicatorsTest {
    private KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new PriceIndicators(300000)),
                event -> event.ticker, Types.STRING);
    }

    private static void send(KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed> h,
                             long time, String ticker, double price) throws Exception {
        h.processElement(new StreamRecord<>(new PriceEvent(time, ticker, "E", price), time));
    }

    private static List<EventProcessed> events(KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed> h) {
        return h.getOutput().stream().filter(x -> x instanceof StreamRecord<?>)
                .map(x -> (EventProcessed) ((StreamRecord<?>) x).getValue()).toList();
    }

    @Test void equalityFromBelowEmitsNoAdvisoryThenUpwardDepartureEmitsBuy() throws Exception {
        try (var h = harness()) {
            h.open();
            for (int i = 0; i < 33; i++) {
                double close = i == 0 ? 39 : i <= 30 ? 0.01 : i == 31 ? 0.6744287344315342 : 1000;
                send(h, i * 300000L + 1000, "AAA.FR", close);
                h.processWatermark(new Watermark((i + 1) * 300000L - 1));
            }
            long equalityTime = 31 * 300000L + 1000;
            long departureTime = 32 * 300000L + 1000;
            var equality = events(h).stream().filter(e -> e.timestamp_emit == equalityTime).toList();
            assertEquals(List.of("price", "EMA38", "EMA100"), equality.stream().map(e -> e.type).toList());
            assertEquals(0.433211324148742, equality.get(1).value);
            assertEquals(equality.get(1).value, equality.get(2).value);
            var departure = events(h).stream().filter(e -> e.timestamp_emit == departureTime).toList();
            assertEquals(List.of("price", "EMA38", "EMA100", "BUY"), departure.stream().map(e -> e.type).toList());
            assertEquals(1000.0, departure.get(3).value);
        }
    }

    @Test void emitsOnlyOnCrossoverIncludingInitialEquality() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1000, "ZERO.FR", 0);
            h.processWatermark(new Watermark(299999));
            send(h, 301000, "ZERO.FR", 39);
            for (int i = 0; i < 33; i++) {
                send(h, (i + 1) * 300000L + 1000, "AAA.FR", i == 0 ? 39 : i < 32 ? 0.01 : 1000);
                h.processWatermark(new Watermark((i + 2) * 300000L - 1));
            }
            var signals = events(h).stream().filter(e -> e.ticker.equals("AAA.FR") && (e.type.equals("BUY") || e.type.equals("SELL"))).toList();
            assertEquals(List.of("BUY", "SELL", "BUY"), signals.stream().map(e -> e.type).toList());
            assertEquals(List.of(301000L, 9301000L, 9901000L), signals.stream().map(e -> e.timestamp_emit).toList());
            assertEquals(List.of(39.0, 0.01, 1000.0), signals.stream().map(e -> e.value).toList());
            assertEquals(1, events(h).stream().filter(e -> e.ticker.equals("ZERO.FR") && e.type.equals("BUY")).count());
        }
    }

    @Test void equalityEmitsNoAdvisoryButDepartureDownwardEmitsSell() throws Exception {
        try (var h = harness()) {
            h.open();
            for (int i = 0; i < 30; i++) {
                send(h, i * 300000L + 1000, "AAA.FR", i == 0 ? 39 : 0.01);
                h.processWatermark(new Watermark((i + 1) * 300000L - 1));
            }
            send(h, 9001000, "AAA.FR", 0.26939267788804794);
            h.processWatermark(new Watermark(9299999));
            var equalityWindow = events(h).stream().filter(e -> e.timestamp_emit == 9001000).toList();
            assertEquals(List.of("price", "EMA38", "EMA100"), equalityWindow.stream().map(e -> e.type).toList());
            assertEquals(0.4334747338241805, equalityWindow.get(1).value);
            assertEquals(equalityWindow.get(1).value, equalityWindow.get(2).value);

            send(h, 9301000, "AAA.FR", 0.01);
            h.processWatermark(new Watermark(9599999));
            var departure = events(h).stream().filter(e -> e.timestamp_emit == 9301000).toList();
            assertEquals(List.of("price", "EMA38", "EMA100", "SELL"), departure.stream().map(e -> e.type).toList());
            assertTrue(departure.get(1).value < departure.get(2).value);
            assertEquals(0.01, departure.get(3).value);
        }
    }

    @Test void closesByEventTimeAndKeepsTickersIndependent() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 200000, "AAA.FR", 100);
            send(h, 100000, "AAA.FR", 50);
            send(h, 250000, "BBB.NL", 200);
            h.processWatermark(new Watermark(299998));
            assertEquals(3, events(h).size());
            h.processWatermark(new Watermark(299999));
            var ema = events(h).stream().filter(e -> e.type.equals("EMA38")).toList();
            assertEquals(2, ema.size());
            assertEquals(100 * 2.0 / 39, ema.stream().filter(e -> e.ticker.equals("AAA.FR")).findFirst().orElseThrow().value, 1e-10);
            assertEquals(200 * 2.0 / 39, ema.stream().filter(e -> e.ticker.equals("BBB.NL")).findFirst().orElseThrow().value, 1e-10);
        }
    }

    @Test void checkpointRestoresPendingWindowsTimersAndEma() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            send(h, 1000, "AAA.FR", 100);
            h.processWatermark(new Watermark(299999));
            send(h, 300000, "AAA.FR", 200);
            snapshot = h.snapshot(1, 1);
        }
        try (var h = harness()) {
            h.initializeState(snapshot);
            h.open();
            h.processWatermark(new Watermark(599999));
            var result = events(h);
            assertEquals(2, result.size());
            assertEquals("EMA38", result.get(0).type);
            assertEquals(200 * 2.0 / 39 + 100 * 2.0 / 39 * (1 - 2.0 / 39), result.get(0).value, 1e-10);
            assertEquals("EMA100", result.get(1).type);
            assertEquals(200 * 2.0 / 101 + 100 * 2.0 / 101 * (1 - 2.0 / 101), result.get(1).value, 1e-10);
        }
    }

    @Test void restoredPendingCrossoverEmitsOnceAndCannotReopen() throws Exception {
        OperatorSubtaskState pending;
        try (var h = harness()) {
            h.open();
            for (int i = 0; i < 30; i++) {
                send(h, i * 300000L + 1000, "AAA.FR", i == 0 ? 39 : 0.01);
                h.processWatermark(new Watermark((i + 1) * 300000L - 1));
            }
            send(h, 9001000, "AAA.FR", 0.01);
            pending = h.snapshot(1, 1);
        }
        OperatorSubtaskState finalized;
        try (var h = harness()) {
            h.initializeState(pending);
            h.open();
            h.processWatermark(new Watermark(9299999));
            assertEquals(1, events(h).stream().filter(e -> e.type.equals("SELL")).count());
            finalized = h.snapshot(2, 2);
        }
        try (var h = harness()) {
            h.initializeState(finalized);
            h.open();
            send(h, 9001000, "AAA.FR", 0.01);
            h.processWatermark(new Watermark(9299999));
            assertEquals(0, events(h).stream().filter(e -> e.type.equals("BUY") || e.type.equals("SELL")).count());
        }
    }

    @Test void lateTicksAreForwardedWithoutRevisingFinalizedEmaAndEmptyWindowsAreSkipped() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1000, "AAA.FR", 100);
            h.processWatermark(new Watermark(899999));
            send(h, 2000, "AAA.FR", 999);
            send(h, 900000, "AAA.FR", 200);
            h.processWatermark(new Watermark(1199999));
            var result = events(h);
            assertEquals(8, result.size());
            assertEquals(2, result.stream().filter(e -> e.type.equals("EMA38")).count());
            assertEquals(200 * 2.0 / 39 + 100 * 2.0 / 39 * (1 - 2.0 / 39), result.get(6).value, 1e-10);
        }
    }

    @Test void recoveryCannotReopenAnAlreadyFinalizedWindowBeforeWatermarksResume() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            send(h, 1000, "AAA.FR", 100);
            h.processWatermark(new Watermark(299999));
            snapshot = h.snapshot(1, 1);
        }
        try (var h = harness()) {
            h.initializeState(snapshot);
            h.open();
            send(h, 2000, "AAA.FR", 999);
            send(h, 300000, "AAA.FR", 200);
            h.processWatermark(new Watermark(599999));
            var result = events(h);
            assertEquals(4, result.size());
            assertEquals(200 * 2.0 / 39 + 100 * 2.0 / 39 * (1 - 2.0 / 39), result.get(2).value, 1e-10);
        }
    }
}
