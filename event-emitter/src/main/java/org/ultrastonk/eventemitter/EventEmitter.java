package org.ultrastonk.eventemitter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class EventEmitter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Print a progress line every N emitted events. */
    private static final long PROGRESS_INTERVAL = 1_000_000L;
    /** Also print a progress line when this many milliseconds have passed since the last one,
     *  so sparse phases (e.g. the pre-open hours) still show liveness between count milestones. */
    private static final long PROGRESS_PRINT_MS = 30_000L;
    /** Upper bound on printed send failures; the total is always reported at the end. */
    private static final int MAX_LOGGED_FAILURES = 10;

    /** Which clock drives the replay schedule. Event time is the unchanged default. */
    public enum PacingMode {
        EVENT, CAPTURE;

        ReplayPacer.Mode toPacer() {
            return this == CAPTURE ? ReplayPacer.Mode.CAPTURE : ReplayPacer.Mode.EVENT;
        }

        static PacingMode fromString(String value) {
            String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
            if (v.isEmpty() || v.equals("event")) {
                return EVENT;
            }
            if (v.equals("capture")) {
                return CAPTURE;
            }
            throw new IllegalArgumentException("PACING must be event or capture, was '" + value + "'");
        }
    }

    /** Policy for a row whose system date/time (capture) columns cannot be read. */
    public enum MissingCaptureTime {
        EVENT_TIME(ReplayPacer.MissingCaptureTime.EVENT_TIME),
        NO_WAIT(ReplayPacer.MissingCaptureTime.NO_WAIT),
        FAIL(ReplayPacer.MissingCaptureTime.FAIL);

        private final ReplayPacer.MissingCaptureTime policy;

        MissingCaptureTime(ReplayPacer.MissingCaptureTime policy) {
            this.policy = policy;
        }

        ReplayPacer.MissingCaptureTime toPacer() {
            return policy;
        }

        static MissingCaptureTime fromString(String value) {
            String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
            return switch (v) {
                case "", "event", "event-time" -> EVENT_TIME;
                case "no-wait", "hold" -> NO_WAIT;
                case "fail" -> FAIL;
                default -> throw new IllegalArgumentException(
                        "MISSING_CAPTURE_TIME must be event, no-wait or fail, was '" + value + "'");
            };
        }
    }

    public record Config(
            List<Path> csvPaths,
            String topic,
            String bootstrapServers,
            double speedMultiplier,
            long maxSleepMs,
            boolean eventRecordTimestamp,
            ZoneId eventZone,
            DataLoader.PriceFilter priceFilter,
            String acks,
            boolean idempotence,
            PacingMode pacingMode,
            MissingCaptureTime missingCaptureTime,
            long capturePrerollMs,
            long captureMeasureMs,
            String pacingReportPath,
            int pacingSampleLimit
    ) {
        public Config {
            if (!Double.isFinite(speedMultiplier) || speedMultiplier <= 0.0) {
                throw new IllegalArgumentException("SPEED_MULTIPLIER must be finite and > 0, was " + speedMultiplier);
            }
            if (maxSleepMs < 0 || maxSleepMs > Long.MAX_VALUE / 1_000_000L) {
                throw new IllegalArgumentException("MAX_SLEEP_MS is outside the supported range");
            }
            if (capturePrerollMs < 0 || captureMeasureMs < 0) {
                throw new IllegalArgumentException("REPLAY_PREROLL_MS and REPLAY_MEASURE_MS must be nonnegative");
            }
            if (pacingSampleLimit < 0) {
                throw new IllegalArgumentException("PACING_SAMPLE_LIMIT must be nonnegative");
            }
        }

        /** Builds the configuration from environment variables, falling back to sane defaults. */
        public static Config fromEnv() {
            return fromEnv(System::getenv);
        }

        static Config fromEnv(Function<String, String> env) {
            String csv = env.apply("CSV_PATH");
            if (csv == null || csv.isBlank()) {
                csv = "../data/src/debs2022-gc-trading-day-12-11-21.csv";
            }
            List<Path> paths = Arrays.stream(csv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(Path::of)
                    .toList();
            if (paths.isEmpty()) {
                throw new IllegalArgumentException("CSV_PATH is empty");
            }
            for (Path path : paths) {
                if (!Files.isReadable(path)) {
                    throw new IllegalArgumentException("CSV file is not readable: " + path.toAbsolutePath()
                            + " (set CSV_PATH)");
                }
            }
            String recordTimestamp = value(env, "RECORD_TIMESTAMP", "append");
            if (!"append".equalsIgnoreCase(recordTimestamp) && !"event".equalsIgnoreCase(recordTimestamp)) {
                throw new IllegalArgumentException("RECORD_TIMESTAMP must be append or event");
            }
            String reportPath = env.apply("PACING_REPORT_PATH");
            return new Config(
                    paths,
                    value(env, "TOPIC_NAME", "price-events"),
                    value(env, "KAFKA_SERVER", "localhost:9092"),
                    Double.parseDouble(value(env, "SPEED_MULTIPLIER", "1.0")),
                    Long.parseLong(value(env, "MAX_SLEEP_MS", "0")),
                    "event".equalsIgnoreCase(recordTimestamp),
                    ZoneId.of(value(env, "EVENT_TIME_ZONE", "Europe/Berlin")),
                    DataLoader.PriceFilter.fromString(value(env, "PRICE_FILTER", "none")),
                    value(env, "KAFKA_ACKS", "all"),
                    Boolean.parseBoolean(value(env, "KAFKA_IDEMPOTENCE", "true")),
                    PacingMode.fromString(value(env, "PACING", "event")),
                    MissingCaptureTime.fromString(value(env, "MISSING_CAPTURE_TIME", "event")),
                    Long.parseLong(value(env, "REPLAY_PREROLL_MS", "0")),
                    Long.parseLong(value(env, "REPLAY_MEASURE_MS", "0")),
                    reportPath == null || reportPath.isBlank() ? null : reportPath.trim(),
                    Integer.parseInt(value(env, "PACING_SAMPLE_LIMIT", "1000"))
            );
        }

        private static String value(Function<String, String> env, String name, String fallback) {
            String value = env.apply(name);
            return (value == null || value.isBlank()) ? fallback : value.trim();
        }
    }

    /**
     * Outcome of a run. {@code firstEventTimeMs} is the time of the first row emitted,
     * {@code load} aggregates the per-file CSV accounting over all replayed files, so
     * {@code load.emitted() == submitted} unless a send failed.
     */
    public record Stats(long submitted, long failed, long firstEventTimeMs, long wallClockMs,
                        DataLoader.LoadStats load) {
        public double eventsPerSecond() {
            return wallClockMs > 0 ? submitted * 1000.0 / wallClockMs : submitted;
        }
    }

    private static final class TimingSample {
        final long ordinal;
        final ReplayPacer.Pace pace;
        final Long captureTimestampMs;
        final long eventTimestampMs;
        final long enqueuedEpochMs;
        volatile Long ackEpochMs;

        TimingSample(long ordinal, ReplayPacer.Pace pace, Long captureTimestampMs, long eventTimestampMs,
                     long enqueuedEpochMs) {
            this.ordinal = ordinal;
            this.pace = pace;
            this.captureTimestampMs = captureTimestampMs;
            this.eventTimestampMs = eventTimestampMs;
            this.enqueuedEpochMs = enqueuedEpochMs;
        }

        ObjectNode json() {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("source_ordinal", ordinal);
            node.put("segment", pace.segment().name());
            if (captureTimestampMs == null) {
                node.putNull("capture_ms");
            } else {
                node.put("capture_ms", captureTimestampMs);
            }
            node.put("event_ms", eventTimestampMs);
            node.put("scheduled_offset_ms", pace.scheduledOffsetMs());
            node.put("actual_offset_ms", pace.actualOffsetMs());
            node.put("enqueued_epoch_ms", enqueuedEpochMs);
            if (ackEpochMs == null) {
                node.putNull("ack_epoch_ms");
            } else {
                node.put("ack_epoch_ms", ackEpochMs);
            }
            return node;
        }
    }

    private final Config config;

    public EventEmitter(Config config) {
        this.config = config;
    }

    /**
     * Streams the CSV and publishes every valid row to the configured topic.
     *
     * <p>Returns once all files have been handed to the producer and flushed.
     */
    public Stats emit() throws IOException, CsvException {
        AtomicLong submitted = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong ordinal = new AtomicLong();
        AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());
        List<TimingSample> samples = Collections.synchronizedList(new ArrayList<>());
        boolean capture = config.pacingMode() == PacingMode.CAPTURE;
        ReplayPacer pacer = new ReplayPacer(config.pacingMode().toPacer(), config.missingCaptureTime().toPacer(),
                config.capturePrerollMs(), config.captureMeasureMs(), config.speedMultiplier(),
                config.maxSleepMs(), ReplayPacer.NanoSource.system())
                .withSampleLimit(config.pacingSampleLimit());
        DataLoader.LoadStats load = null;
        long runStartMs = System.currentTimeMillis();

        System.out.printf("Replaying %s -> topic '%s' via %s at %.2fx (event zone %s, price filter %s, pacing %s)%n",
                config.csvPaths(), config.topic(), config.bootstrapServers(), config.speedMultiplier(),
                config.eventZone(), config.priceFilter().id(), config.pacingMode());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            // A single pacer spans all files: the per-day files are chronologically ordered,
            // so the replay stays gap-free across files.
            for (Path csv : config.csvPaths()) {
                System.out.printf("Replaying %s%n", csv.toAbsolutePath());
                DataLoader.LoadStats fileStats = replayFile(producer, csv, pacer, capture, samples, ordinal,
                        submitted, failed, runStartMs, lastProgressMs);
                load = load == null ? fileStats : load.plus(fileStats);
                System.out.printf("  %s: %s%n", csv.getFileName(), fileStats.summary(config.priceFilter()));
            }
            producer.flush();
        }

        if (load == null) {
            load = DataLoader.LoadStats.empty();
        }
        Stats stats = new Stats(submitted.get(), failed.get(), pacer.firstTimeMs(),
                System.currentTimeMillis() - runStartMs, load);
        System.out.printf("Done: %d submitted, %d failed, %.0f ev/s, first paced time %d%n",
                stats.submitted(), stats.failed(), stats.eventsPerSecond(), stats.firstEventTimeMs());
        System.out.printf("Total: %s%n", load.summary(config.priceFilter()));
        if (config.priceFilter() != DataLoader.PriceFilter.NONE) {
            System.out.printf("NOTE: PRICE_FILTER=%s dropped %,d of %,d CSV rows; the topic does NOT hold a "
                            + "faithful replay, do not use this run for report numbers.%n",
                    config.priceFilter().id(), load.droppedTotal(), load.rowsRead());
        }
        if (config.pacingReportPath() != null) {
            writePacingReport(pacer, samples, submitted.get(), failed.get(), stats);
        }
        return stats;
    }

    private void writePacingReport(ReplayPacer pacer, List<TimingSample> samples, long submitted, long failed,
                                   Stats stats) throws IOException {
        ObjectNode report = pacer.report();
        report.put("pacing_report_version", 1);
        report.put("topic", config.topic());
        report.put("speed_multiplier", config.speedMultiplier());
        report.put("max_sleep_ms", config.maxSleepMs());
        report.put("event_time_zone", config.eventZone().getId());
        report.put("price_filter", config.priceFilter().id());
        report.put("submitted", submitted);
        report.put("failed", failed);
        report.put("first_paced_time_ms", stats.firstEventTimeMs());
        report.put("wall_clock_ms", stats.wallClockMs());
        ArrayNode sampleArray = report.putArray("samples");
        synchronized (samples) {
            for (TimingSample sample : samples) {
                sampleArray.add(sample.json());
            }
        }
        Path path = Path.of(config.pacingReportPath());
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), report);
        System.out.printf("Pacing report written to %s%n", path.toAbsolutePath());
    }

    /** Streams one CSV file into the producer and returns the CSV-side accounting for that file. */
    private DataLoader.LoadStats replayFile(KafkaProducer<String, String> producer, Path csv, ReplayPacer pacer,
                                            boolean capture, List<TimingSample> samples, AtomicLong ordinal,
                                            AtomicLong submitted, AtomicLong failed, long runStartMs,
                                            AtomicLong lastProgressMs)
            throws IOException, CsvException {
        DataLoader.Counters counters = DataLoader.Counters.of();
        new DataLoader(csv, config.eventZone(), config.priceFilter(), capture).load(counters, event -> {
            try {
                ReplayPacer.Pace pace = pacer.pace(event.captureTimestampMs(), event.eventTimestampMs());
                long sourceOrdinal = ordinal.getAndIncrement();
                TimingSample sample = null;
                if (samples.size() < config.pacingSampleLimit()) {
                    sample = new TimingSample(sourceOrdinal, pace, event.captureTimestampMs(),
                            event.eventTimestampMs(), System.currentTimeMillis());
                    samples.add(sample);
                }

                RawPriceEvent outgoing = event.withEmitTimestamp(System.currentTimeMillis());
                if (capture) {
                    outgoing = outgoing.withSource(sourceOrdinal, event.captureTimestampMs());
                }
                String key = outgoing.key();
                String payload = MAPPER.writeValueAsString(outgoing);

                Long recordTimestamp = config.eventRecordTimestamp() ? outgoing.eventTimestampMs() : null;
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        config.topic(),
                        null,                            // let the partitioner hash the key
                        recordTimestamp,
                        key,
                        payload
                );

                TimingSample acknowledged = sample;
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        long failures = failed.incrementAndGet();
                        if (failures <= MAX_LOGGED_FAILURES) {
                            System.err.println("Emit failed for key " + key + ": " + ex.getMessage());
                        }
                    } else if (acknowledged != null) {
                        acknowledged.ackEpochMs = System.currentTimeMillis();
                    }
                });

                long sent = submitted.incrementAndGet();
                long nowMs = System.currentTimeMillis();
                if (sent % PROGRESS_INTERVAL == 0 || nowMs - lastProgressMs.get() >= PROGRESS_PRINT_MS) {
                    lastProgressMs.set(nowMs);
                    long elapsedMs = Math.max(1, nowMs - runStartMs);
                    System.out.printf("... %d events sent (%.0f ev/s, %.1f s elapsed)%n",
                            sent, sent * 1000.0 / elapsedMs, elapsedMs / 1000.0);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while replaying " + csv, e);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Could not serialise event " + event, e);
            }
        });
        return counters.toStats();
    }

    /** Producer tuning: batched, compressed and (by default) idempotent for per-partition ordering. */
    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Ordering: idempotence preserves per-partition order across retries, but Kafka
        // requires acks=all for it, so it is only enabled when that holds.
        boolean idempotence = config.idempotence() && "all".equalsIgnoreCase(config.acks());
        props.put(ProducerConfig.ACKS_CONFIG, idempotence ? "all" : config.acks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotence));

        // A dead broker should surface as an error instead of hanging the replay forever.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "120000");
        return props;
    }

    public static void main(String[] args) throws IOException, CsvException {
        Stats stats = new EventEmitter(Config.fromEnv()).emit();
        if (stats.failed() > 0) {
            System.exit(1);
        }
    }
}
