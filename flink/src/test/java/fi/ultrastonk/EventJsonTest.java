package fi.ultrastonk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EventJsonTest {
    private static final String VALID = "{\"timestamp_emit\":123,\"ticker\":\"BRK.B.FR\",\"sec_type\":\"E\",\"price\":42.5}";

    @Test void parsesInputAndProducesOnlyGatewayFields() throws Exception {
        PriceEvent event = EventJson.parse(VALID).trade;
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

    @Test void parsesReplayTradeUsingExchangeTimeRatherThanEmissionTime() throws Exception {
        PriceEvent event = EventJson.parse("""
                {"id":"BRK.B.FR","secType":"E","last":42.5,"bid":42,"ask":43,
                 "eventTimestampMs":1636351200021,"emitTimestampMs":1792152000100,"tradingDate":"08-11-2021"}
                """).trade;
        assertEquals(1636351200021L, event.timestamp_emit);
        assertEquals("BRK.B.FR", event.ticker);
        assertEquals("E", event.sec_type);
        assertEquals(42.5, event.price);
    }

    @Test void tombstonesReachValidationAsInvalidJsonValues() throws Exception {
        assertTrue(new PriceValueSchema().deserialize(null).rejection.contains("\"raw\":\"null\""));
        assertThrows(IllegalArgumentException.class, () -> EventJson.parse("null"));
    }
}
