package org.ultrastonk.eventemitter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventEmitterConfigTest {

    @TempDir
    Path tempDir;
    private String csv;

    @BeforeEach
    void readableCsv() throws IOException {
        csv = Files.createFile(tempDir.resolve("fixture.csv")).toString();
    }

    private EventEmitter.Config config(String... pairs) {
        Map<String, String> values = new HashMap<>();
        values.put("CSV_PATH", csv);
        for (int i = 0; i < pairs.length; i += 2) {
            values.put(pairs[i], pairs[i + 1]);
        }
        Function<String, String> env = values::get;
        return EventEmitter.Config.fromEnv(env);
    }

    @Test
    void pacingDefaultsStayOnExchangeEventTime() {
        EventEmitter.Config config = config();

        assertEquals(EventEmitter.PacingMode.EVENT, config.pacingMode());
        assertEquals(EventEmitter.MissingCaptureTime.EVENT_TIME, config.missingCaptureTime());
        assertEquals(0, config.capturePrerollMs());
        assertEquals(0, config.captureMeasureMs());
        assertNull(config.pacingReportPath());
        assertEquals(1000, config.pacingSampleLimit());
    }

    @Test
    void capturePacingIsOptInAndSelectsTheMissingTimePolicy() {
        EventEmitter.Config config = config(
                "PACING", "capture",
                "MISSING_CAPTURE_TIME", "no-wait",
                "REPLAY_PREROLL_MS", "60000",
                "REPLAY_MEASURE_MS", "300000",
                "PACING_REPORT_PATH", "/results/pacing.json",
                "PACING_SAMPLE_LIMIT", "25");

        assertEquals(EventEmitter.PacingMode.CAPTURE, config.pacingMode());
        assertEquals(EventEmitter.MissingCaptureTime.NO_WAIT, config.missingCaptureTime());
        assertEquals(60_000, config.capturePrerollMs());
        assertEquals(300_000, config.captureMeasureMs());
        assertEquals("/results/pacing.json", config.pacingReportPath());
        assertEquals(25, config.pacingSampleLimit());
    }

    @Test
    void invalidPacingSettingsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> config("PACING", "wall-clock"));
        assertThrows(IllegalArgumentException.class, () -> config("MISSING_CAPTURE_TIME", "guess"));
        assertThrows(IllegalArgumentException.class, () -> config("REPLAY_PREROLL_MS", "-1"));
        assertThrows(IllegalArgumentException.class, () -> config("REPLAY_MEASURE_MS", "-5"));
        assertThrows(IllegalArgumentException.class, () -> config("PACING_SAMPLE_LIMIT", "-1"));
    }
}
