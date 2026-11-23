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

    static PriceInput parse(String raw) throws IOException {
        JsonNode node = MAPPER.readTree(raw);
        if (node == null || !node.isObject()) throw new IllegalArgumentException("expected a JSON object");
        // Raw CSV replay uses exchange time; normalized synthetic fixtures keep their existing schema.
        boolean replay = node.has("eventTimestampMs") || node.has("id") || node.has("secType");
        String timestampField = replay ? "eventTimestampMs" : "timestamp_emit";
        String priceField = replay ? "last" : "price";
        JsonNode timestamp = node.path(timestampField);
        JsonNode ticker = node.path(replay ? "id" : "ticker");
        JsonNode secType = node.path(replay ? "secType" : "sec_type");
        JsonNode price = node.path(priceField);
        if (!timestamp.isIntegralNumber() || !timestamp.canConvertToLong()
                || timestamp.longValue() < 0 || timestamp.longValue() > Long.MAX_VALUE - 172_800_000L) {
            throw new IllegalArgumentException(timestampField + " must be nonnegative epoch milliseconds");
        }
        if (!ticker.isTextual() || !TICKER.matcher(ticker.textValue()).matches()) {
            throw new IllegalArgumentException("ticker must have the form STOCK.EXCHANGE");
        }
        if (!secType.isTextual() || secType.textValue().isBlank()) {
            throw new IllegalArgumentException("sec_type must be a nonempty string");
        }
        // Quote and housekeeping rows are valid raw events but do not carry a trade price.
        if (replay && (price.isMissingNode() || price.isNull())) return new PriceInput(timestamp.longValue(), null, null);
        if (!price.isNumber() || !Double.isFinite(price.doubleValue()) || price.doubleValue() < 0) {
            throw new IllegalArgumentException(priceField + " must be a finite nonnegative number");
        }
        if (replay && price.doubleValue() == 0) return new PriceInput(timestamp.longValue(), null, null);
        return new PriceInput(timestamp.longValue(),
                new PriceEvent(timestamp.longValue(), ticker.textValue(), secType.textValue(), price.doubleValue()), null);
    }

    static String write(Object event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize event", e);
        }
    }

    static String rejected(String raw, String reason) {
        // JSON escaping can enlarge an already large Kafka input beyond the rejected topic's limit.
        if (raw.length() > 32_768) {
            return write(Map.of("raw", raw.substring(0, 32_768), "reason", reason, "truncated", true));
        }
        return write(Map.of("raw", raw, "reason", reason));
    }
}
