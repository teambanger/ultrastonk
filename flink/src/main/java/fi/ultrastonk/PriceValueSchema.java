package fi.ultrastonk;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

/** Kafka tombstones are invalid price values and must reach validation instead of crashing a reader. */
public final class PriceValueSchema extends SimpleStringSchema {
    private transient ComponentProfiler profiler;
    @Override public void open(org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext context) {
        profiler = new ComponentProfiler(context.getMetricGroup(), "source_decode");
    }
    @Override public String deserialize(byte[] message) {
        long started = profiler == null ? -1 : profiler.begin();
        try {
            return message == null ? "null" : super.deserialize(message);
        } finally {
            if (profiler != null) profiler.end(started);
        }
    }
}
