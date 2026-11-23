package org.ultrastonk.eventemitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataLoaderCaptureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int COLUMNS = 40;

    @TempDir
    Path tempDir;

    private static String row(String id, String systemDate, String systemTime, String tradingDate,
                              String tradingTime, String last) {
        String[] columns = new String[COLUMNS];
        for (int i = 0; i < COLUMNS; i++) {
            columns[i] = "";
        }
        columns[0] = id;
        columns[1] = "E";
        columns[2] = systemDate;
        columns[3] = systemTime;
        columns[21] = last;
        columns[23] = tradingTime;
        columns[26] = tradingDate;
        return String.join(",", columns);
    }

    private Path fixture(String... rows) throws IOException {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 13; i++) {
            content.append("# header ").append(i).append('\n');
        }
        for (String row : rows) {
            content.append(row).append('\n');
        }
        Path path = tempDir.resolve("capture.csv");
        Files.writeString(path, content);
        return path;
    }

    private static List<RawPriceEvent> load(Path path, boolean captureTime) throws Exception {
        List<RawPriceEvent> events = new ArrayList<>();
        new DataLoader(path, ZoneOffset.UTC, DataLoader.PriceFilter.NONE, captureTime)
                .load(DataLoader.Counters.of(), events::add);
        return events;
    }

    @Test
    void keepsExchangeAndCaptureTimestampsDistinct() throws Exception {
        Path path = fixture(row("A.FR", "08-11-2021", "10:00:00.000", "", "09:59:59.500", "10.50"));

        List<RawPriceEvent> events = load(path, true);

        assertEquals(1, events.size());
        long expectedEvent = DataLoader.parseEventTimestamp("08-11-2021", "09:59:59.500", ZoneOffset.UTC);
        long expectedCapture = DataLoader.parseEventTimestamp("08-11-2021", "10:00:00.000", ZoneOffset.UTC);
        assertEquals(expectedEvent, events.get(0).eventTimestampMs(),
                "the payload keeps the exchange/trading time, not the capture time");
        assertEquals(expectedCapture, events.get(0).captureTimestampMs());
    }

    @Test
    void fallsBackToSystemTimeForEventTimeButKeepsCaptureTime() throws Exception {
        Path path = fixture(row("B.FR", "08-11-2021", "10:00:00.010", "", "", "11.00"));

        RawPriceEvent event = load(path, true).get(0);

        assertEquals(DataLoader.parseEventTimestamp("08-11-2021", "10:00:00.010", ZoneOffset.UTC),
                event.eventTimestampMs());
        assertEquals(event.eventTimestampMs(), event.captureTimestampMs().longValue());
    }

    @Test
    void missingCaptureTimeIsReportedWithoutDroppingTheRow() throws Exception {
        Path path = fixture(row("C.FR", "", "", "08-11-2021", "11:00:00.000", "12.00"));

        List<RawPriceEvent> events = load(path, true);

        assertEquals(1, events.size(), "a row with an exchange time is still replayed");
        assertEquals(DataLoader.parseEventTimestamp("08-11-2021", "11:00:00.000", ZoneOffset.UTC),
                events.get(0).eventTimestampMs());
        assertNull(events.get(0).captureTimestampMs());
    }

    @Test
    void captureTimeIsNotParsedUnlessCapturePacingIsRequested() throws Exception {
        Path path = fixture(row("A.FR", "08-11-2021", "10:00:00.000", "", "09:59:59.500", "10.50"));

        assertNull(load(path, false).get(0).captureTimestampMs());
    }

    @Test
    void defaultPayloadIsUnchangedAndCaptureIdentityIsOptIn() throws Exception {
        Path path = fixture(row("A.FR", "08-11-2021", "10:00:00.000", "", "09:59:59.500", "10.50"));
        RawPriceEvent plainEvent = load(path, false).get(0);
        RawPriceEvent event = load(path, true).get(0);

        JsonNode plain = MAPPER.readTree(MAPPER.writeValueAsString(plainEvent.withEmitTimestamp(42L)));
        assertFalse(plain.has("captureTimestampMs"), "default payloads must not gain capture fields");
        assertFalse(plain.has("sourceOrdinal"));
        assertEquals(event.eventTimestampMs(), plain.get("eventTimestampMs").asLong());

        JsonNode tagged = MAPPER.readTree(MAPPER.writeValueAsString(
                event.withEmitTimestamp(42L).withSource(7L, event.captureTimestampMs())));
        assertTrue(tagged.has("captureTimestampMs"));
        assertEquals(7, tagged.get("sourceOrdinal").asLong());
        assertEquals(event.eventTimestampMs(), tagged.get("eventTimestampMs").asLong(),
                "adding identity fields must not replace query event time");
    }
}
