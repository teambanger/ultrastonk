package fi.ultrastonk;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PoliciesTest {
    private KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed> harness(
            Policies.Deduplication dedup, Policies.LateEvents late) throws Exception {
        var options = new Policies(dedup, late, 1000, 200, 10000);
        var h = new KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed>(
                new KeyedProcessOperator<>(new PriceIndicators(1000, options)), event -> event.ticker, Types.STRING);
        h.open();
        return h;
    }

    private static void send(KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed> h,
                             long time, double price) throws Exception {
        h.processElement(new StreamRecord<>(new PriceEvent(time, "AAA.FR", "E", price), time));
    }

    private static List<EventProcessed> events(KeyedOneInputStreamOperatorTestHarness<String, PriceEvent, EventProcessed> h) {
        return h.getOutput().stream().filter(x -> x instanceof StreamRecord<?>)
                .map(x -> (EventProcessed) ((StreamRecord<?>) x).getValue()).toList();
    }

    @Test void exactDedupRetainsDifferentPricesAtSameTimestamp() throws Exception {
        try (var h = harness(Policies.Deduplication.EXACT, Policies.LateEvents.DROP)) {
            send(h, 100, 10);
            send(h, 100, 10);
            send(h, 100, 20);
            h.processWatermark(new Watermark(999));
            assertEquals(4, events(h).size());
            assertEquals(20 * 2.0 / 39, events(h).get(2).value, 1e-10);
        }
    }

    @Test void timestampDedupTreatsSameTimeCorrectionsAsDuplicates() throws Exception {
        try (var h = harness(Policies.Deduplication.TIMESTAMP, Policies.LateEvents.DROP)) {
            send(h, 100, 10);
            send(h, 100, 20);
            h.processWatermark(new Watermark(999));
            assertEquals(3, events(h).size());
            assertEquals(10 * 2.0 / 39, events(h).get(1).value, 1e-10);
        }
    }

    @Test void dedupExpiresInProcessingTime() throws Exception {
        try (var h = harness(Policies.Deduplication.EXACT, Policies.LateEvents.DROP)) {
            h.setStateTtlProcessingTime(0);
            send(h, 100, 10);
            h.setStateTtlProcessingTime(999);
            send(h, 100, 10);
            h.setStateTtlProcessingTime(1001);
            send(h, 100, 10);
            assertEquals(2, events(h).size());
        }
    }

    @Test void graceIncludesDelayedCloseAndQuarantinesBeyondGrace() throws Exception {
        try (var h = harness(Policies.Deduplication.NONE, Policies.LateEvents.GRACE)) {
            send(h, 100, 10);
            h.processWatermark(new Watermark(1099));
            send(h, 900, 20);
            assertEquals(2, events(h).size());
            h.processWatermark(new Watermark(1199));
            assertEquals(4, events(h).size());
            assertEquals(20 * 2.0 / 39, events(h).get(2).value, 1e-10);
            send(h, 950, 99);
            assertEquals(1, h.getSideOutput(PriceIndicators.LATE).size());
            assertEquals("price", events(h).get(4).type);
        }
    }

    @Test void sideOutputPreservesPriceButAuditsExcludedLateTick() throws Exception {
        try (var h = harness(Policies.Deduplication.NONE, Policies.LateEvents.SIDE_OUTPUT)) {
            h.processWatermark(new Watermark(999));
            send(h, 100, 10);
            assertEquals(1, events(h).size());
            assertEquals(1, h.getSideOutput(PriceIndicators.LATE).size());
        }
    }

    @Test void invalidJsonIsQuarantinedAndNextValidRecordIsProcessed() throws Exception {
        try (var h = new OneInputStreamOperatorTestHarness<String, PriceEvent>(new ProcessOperator<>(new ParsePriceEvent()))) {
            h.open();
            h.processElement(new StreamRecord<>("not json"));
            h.processElement(new StreamRecord<>("{\"timestamp_emit\":100,\"ticker\":\"AAA.FR\",\"sec_type\":\"E\",\"price\":10}"));
            assertEquals(1, h.getSideOutput(ParsePriceEvent.REJECTED).size());
            assertEquals(1, h.getOutput().size());
        }
    }
}
