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

    @Test void lateTicksAreForwardedWithoutRevisingFinalizedEmaAndEmptyWindowsAreSkipped() throws Exception {
        try (var h = harness()) {
            h.open();
            send(h, 1000, "AAA.FR", 100);
            h.processWatermark(new Watermark(899999));
            send(h, 2000, "AAA.FR", 999);
            send(h, 900000, "AAA.FR", 200);
            h.processWatermark(new Watermark(1199999));
            var result = events(h);
            assertEquals(7, result.size());
            assertEquals(2, result.stream().filter(e -> e.type.equals("EMA38")).count());
            assertEquals(200 * 2.0 / 39 + 100 * 2.0 / 39 * (1 - 2.0 / 39), result.get(5).value, 1e-10);
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
