package org.ultrastonk.eventemitter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EventEmitter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Print a progress line every N emitted events. */
    private static final long PROGRESS_INTERVAL = 1_000_000L;
    /** Also print a progress line when this many milliseconds have passed since the last one,
     *  so sparse phases (e.g. the pre-open hours) still show liveness between count milestones. */
    private static final long PROGRESS_PRINT_MS = 30_000L;
    /** Upper bound on printed send failures; the total is always reported at the end. */
    private static final int MAX_LOGGED_FAILURES = 10;

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
            boolean idempotence
    ) {
        public Config {
            if (speedMultiplier <= 0.0) {
                throw new IllegalArgumentException("SPEED_MULTIPLIER must be > 0, was " + speedMultiplier);
            }
        }

        /** Builds the configuration from environment variables, falling back to sane defaults. */
        public static Config fromEnv() {
            String csv = env("CSV_PATH", "../data/src/debs2022-gc-trading-day-08-11-21.csv");
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
            return new Config(
                    paths,
                    env("TOPIC_NAME", "raw-trade-events"),
                    env("KAFKA_SERVER", "localhost:9092"),
                    Double.parseDouble(env("SPEED_MULTIPLIER", "1.0")),
                    Long.parseLong(env("MAX_SLEEP_MS", "0")),
                    !"append".equalsIgnoreCase(env("RECORD_TIMESTAMP", "event")),
                    ZoneId.of(env("EVENT_TIME_ZONE", "Europe/Berlin")),
                    DataLoader.PriceFilter.fromString(env("PRICE_FILTER", "none")),
                    env("KAFKA_ACKS", "all"),
                    Boolean.parseBoolean(env("KAFKA_IDEMPOTENCE", "true"))
            );
        }

        private static String env(String name, String fallback) {
            String value = System.getenv(name);
            return (value == null || value.isBlank()) ? fallback : value.trim();
        }
    }

    /**
     * Outcome of a run. {@code firstEventTimeMs} is the event time of the first row emitted,
     * {@code load} aggregates the per-file CSV accounting over all replayed files, so
     * {@code load.emitted() == submitted} unless a send failed.
     */
    public record Stats(long submitted, long failed, long firstEventTimeMs, long wallClockMs,
                        DataLoader.LoadStats load) {
        public double eventsPerSecond() {
            return wallClockMs > 0 ? submitted * 1000.0 / wallClockMs : submitted;
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
        AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());
        ReplayClock clock = new ReplayClock();
        DataLoader.LoadStats load = null;
        long runStartMs = System.currentTimeMillis();

        System.out.printf("Replaying %s -> topic '%s' via %s at %.2fx (event zone %s, price filter %s)%n",
                config.csvPaths(), config.topic(), config.bootstrapServers(), config.speedMultiplier(),
                config.eventZone(), config.priceFilter().id());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            // A single ReplayClock spans all files: the per-day files are chronologically ordered,
            // so event times keep increasing across files and the replay stays gap-free.
            for (Path csv : config.csvPaths()) {
                System.out.printf("Replaying %s%n", csv.toAbsolutePath());
                DataLoader.LoadStats fileStats =
                        replayFile(producer, csv, clock, submitted, failed, runStartMs, lastProgressMs);
                load = load == null ? fileStats : load.plus(fileStats);
                System.out.printf("  %s: %s%n", csv.getFileName(), fileStats.summary(config.priceFilter()));
            }
            producer.flush();
        }

        if (load == null) {
            load = DataLoader.LoadStats.empty();
        }
        Stats stats = new Stats(submitted.get(), failed.get(), clock.firstEventTimeMs(),
                System.currentTimeMillis() - runStartMs, load);
        System.out.printf("Done: %d submitted, %d failed, %.0f ev/s, first event time %d%n",
                stats.submitted(), stats.failed(), stats.eventsPerSecond(), stats.firstEventTimeMs());
        System.out.printf("Total: %s%n", load.summary(config.priceFilter()));
        if (config.priceFilter() != DataLoader.PriceFilter.NONE) {
            System.out.printf("NOTE: PRICE_FILTER=%s dropped %,d of %,d CSV rows; the topic does NOT hold a "
                            + "faithful replay, do not use this run for report numbers.%n",
                    config.priceFilter().id(), load.droppedTotal(), load.rowsRead());
        }
        return stats;
    }

    /** Streams one CSV file into the producer and returns the CSV-side accounting for that file. */
    private DataLoader.LoadStats replayFile(KafkaProducer<String, String> producer, Path csv, ReplayClock clock,
                                            AtomicLong submitted, AtomicLong failed, long runStartMs,
                                            AtomicLong lastProgressMs)
            throws IOException, CsvException {
        DataLoader.Counters counters = DataLoader.Counters.of();
        new DataLoader(csv, config.eventZone(), config.priceFilter()).load(counters, event -> {
            try {
                clock.pace(event.eventTimestampMs(), config.speedMultiplier(), config.maxSleepMs());

                String key = event.key();
                String payload = MAPPER.writeValueAsString(event.withEmitTimestamp(System.currentTimeMillis()));

                Long recordTimestamp = config.eventRecordTimestamp() ? event.eventTimestampMs() : null;
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        config.topic(),
                        null,                            // let the partitioner hash the key
                        recordTimestamp,
                        key,
                        payload
                );

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        long failures = failed.incrementAndGet();
                        if (failures <= MAX_LOGGED_FAILURES) {
                            System.err.println("Emit failed for key " + key + ": " + ex.getMessage());
                        }
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

    /**
     * Drift-free pacing clock. Every sleep is measured against the fixed instant the
     * replay started at rather than against the previous record, so the replay never
     * accumulates scheduling drift even when individual sends are slow.
     *
     * <p>When a sleep is capped by {@code maxSleepMs}, the schedule is <em>re-based</em>
     * (the start instant shifted forward by the skipped time): without that, every event
     * after the gap would still measure itself against the un-capped target and sleep
     * the full cap again, turning the post-gap burst into one capped sleep per event.
     */
    static final class ReplayClock {
        private long firstEventTimeMs = -1L;
        private long startNano;

        /** Blocks until wall-clock time has caught up with this event's replay slot. */
        void pace(long eventTimeMs, double speedFactor, long maxSleepMs) throws InterruptedException {
            if (firstEventTimeMs < 0) {
                firstEventTimeMs = eventTimeMs;
                startNano = System.nanoTime();
                return;
            }
            // Rows that are out of order in the file are emitted immediately.
            long offsetMs = Math.max(0L, eventTimeMs - firstEventTimeMs);
            long targetNanos = (long) (offsetMs * 1_000_000L / speedFactor);
            long sleepNanos = targetNanos - (System.nanoTime() - startNano);
            if (sleepNanos <= 0) {
                return;
            }
            long capNanos = maxSleepMs > 0 ? maxSleepMs * 1_000_000L : Long.MAX_VALUE;
            if (sleepNanos > capNanos) {
                // Skip the gap instead of just shortening this one sleep: shift the schedule
                // forward so the next events pace relative to the capped point, not the gap.
                startNano += sleepNanos - capNanos;
                sleepNanos = capNanos;
            }
            TimeUnit.NANOSECONDS.sleep(sleepNanos);
        }

        long firstEventTimeMs() {
            return firstEventTimeMs;
        }
    }

    public static void main(String[] args) throws IOException, CsvException {
        Stats stats = new EventEmitter(Config.fromEnv()).emit();
        if (stats.failed() > 0) {
            System.exit(1);
        }
    }
}
