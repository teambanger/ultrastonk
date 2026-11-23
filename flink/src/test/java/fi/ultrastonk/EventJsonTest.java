package fi.ultrastonk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EventJsonTest {
    private static final String VALID = "{\"timestamp_emit\":123,\"ticker\":\"BRK.B.FR\",\"sec_type\":\"E\",\"price\":42.5}";

    @Test void parsesInputAndProducesOnlyGatewayFields() throws Exception {
        PriceEvent event = EventJson.parse(VALID);
        assertEquals("BRK.B.FR", event.ticker);
        String json = EventJson.write(new EventProcessed(event.timestamp_emit, event.ticker, "price", event.price));
        assertEquals("{\"timestamp_emit\":123,\"ticker\":\"BRK.B.FR\",\"type\":\"price\",\"value\":42.5}", json);
    }

    @Test void rejectsInvalidContracts() {
        for (String raw : new String[]{"{}", "null", "[]", VALID.replace("123", "1.2"),
                VALID.replace("42.5", "\"42.5\""), VALID.replace("42.5", "1e999"),
                VALID.replace("123", "-1"), VALID.replace("BRK.B.FR", "AAA"), VALID.replace("42.5", "-2"),
                VALID + " {}", VALID.replace("\"price\":42.5", "\"price\":42.5,\"price\":20")}) {
            assertThrows(Exception.class, () -> EventJson.parse(raw), raw);
        }
    }

    @Test void tombstonesReachValidationAsInvalidJsonValues() throws Exception {
        assertEquals("null", new PriceValueSchema().deserialize(null));
        assertThrows(IllegalArgumentException.class, () -> EventJson.parse("null"));
    }
}
