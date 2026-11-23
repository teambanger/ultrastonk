package fi.ultrastonk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public final class PriceValueSchema extends AbstractDeserializationSchema<PriceInput> {
    private transient ComponentProfiler decode;
    private transient ComponentProfiler validation;

    @Override public void open(org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext context) {
        decode = new ComponentProfiler(context.getMetricGroup(), "source_decode");
        validation = new ComponentProfiler(context.getMetricGroup(), "validation");
    }

    @Override public PriceInput deserialize(byte[] message) {
        long started = decode == null ? -1 : decode.begin();
        String raw = message == null ? "null" : new String(message, StandardCharsets.UTF_8);
        if (decode != null) decode.end(started);
        started = validation == null ? -1 : validation.begin();
        try {
            return EventJson.parse(raw);
        } catch (IOException | IllegalArgumentException e) {
            String reason = e.getMessage() == null ? "invalid JSON value" : e.getMessage();
            return new PriceInput(Long.MIN_VALUE, null, EventJson.rejected(raw,
                    reason.substring(0, Math.min(reason.length(), 1024))));
        } finally {
            if (validation != null) validation.end(started);
        }
    }
}
