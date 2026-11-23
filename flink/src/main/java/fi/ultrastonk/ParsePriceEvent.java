package fi.ultrastonk;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;

public class ParsePriceEvent extends ProcessFunction<String, PriceEvent> {
    public static final OutputTag<String> REJECTED = new OutputTag<String>("invalid-price-events") {};
    private transient Counter received;
    private transient Counter rejected;
    private transient ComponentProfiler profiler;

    @Override public void open(OpenContext context) {
        received = getRuntimeContext().getMetricGroup().addGroup("pipeline").counter("receivedEvents");
        rejected = getRuntimeContext().getMetricGroup().addGroup("pipeline").counter("invalidEvents");
        profiler = new ComponentProfiler(getRuntimeContext().getMetricGroup(), "validation");
    }

    @Override
    public void processElement(String raw, Context ctx, Collector<PriceEvent> out) {
        received.inc();
        PriceEvent event;
        String error = null;
        long started = profiler.begin();
        try {
            event = EventJson.parse(raw);
        } catch (java.io.IOException | IllegalArgumentException e) {
            event = null;
            error = e.getMessage();
        } finally {
            profiler.end(started);
        }
        if (error != null) {
            rejected.inc();
            ctx.output(REJECTED, EventJson.rejected(raw, error));
            return;
        }
        event.ingestionTimeMs = System.currentTimeMillis();
        out.collect(event);
    }
}
