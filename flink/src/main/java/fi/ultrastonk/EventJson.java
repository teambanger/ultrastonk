package fi.ultrastonk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

final class EventJson {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final Pattern TICKER = Pattern.compile("[^\\s.]+(?:\\.[^\\s.]+)*\\.[^\\s.]+");

    private EventJson() {}

    static PriceEvent parse(String raw) throws IOException {
        JsonNode node = MAPPER.readTree(raw);
        if (node == null || !node.isObject()) throw new IllegalArgumentException("expected a JSON object");
        JsonNode timestamp = node.path("timestamp_emit");
        JsonNode ticker = node.path("ticker");
        JsonNode secType = node.path("sec_type");
        JsonNode price = node.path("price");
        if (!timestamp.isIntegralNumber() || !timestamp.canConvertToLong()
                || timestamp.longValue() < 0 || timestamp.longValue() > Long.MAX_VALUE - 172_800_000L) {
            throw new IllegalArgumentException("timestamp_emit must be nonnegative epoch milliseconds");
        }
        if (!ticker.isTextual() || !TICKER.matcher(ticker.textValue()).matches()) {
            throw new IllegalArgumentException("ticker must have the form STOCK.EXCHANGE");
        }
        if (!secType.isTextual() || secType.textValue().isBlank()) {
            throw new IllegalArgumentException("sec_type must be a nonempty string");
        }
        if (!price.isNumber() || !Double.isFinite(price.doubleValue()) || price.doubleValue() < 0) {
            throw new IllegalArgumentException("price must be a finite nonnegative number");
        }
        return new PriceEvent(timestamp.longValue(), ticker.textValue(), secType.textValue(), price.doubleValue());
    }

    static String write(Object event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize event", e);
        }
    }

    static String rejected(String raw, String reason) {
        return write(Map.of("raw", raw, "reason", reason));
    }
}
