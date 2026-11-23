package fi.ultrastonk;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;

public class ParsePriceEvent extends ProcessFunction<PriceInput, PriceEvent> {
    public static final OutputTag<String> REJECTED = new OutputTag<String>("invalid-price-events") {};
    private transient Counter received;
    private transient Counter rejected;

    @Override public void open(OpenContext context) {
        received = getRuntimeContext().getMetricGroup().addGroup("pipeline").counter("receivedEvents");
        rejected = getRuntimeContext().getMetricGroup().addGroup("pipeline").counter("invalidEvents");
    }

    @Override
    public void processElement(PriceInput input, Context ctx, Collector<PriceEvent> out) {
        received.inc();
        if (input.rejection != null) {
            rejected.inc();
            ctx.output(REJECTED, input.rejection);
            return;
        }
        PriceEvent event = input.trade;
        if (event == null) return;
        event.ingestionTimeMs = System.currentTimeMillis();
        out.collect(event);
    }
}
