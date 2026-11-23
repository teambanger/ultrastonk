package fi.ultrastonk;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class PricePipeline {
    public static void main(String[] args) throws Exception {
        Policies policies = Policies.fromEnvironment();
        String brokers = setting("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String prefix = setting("KAFKA_TRANSACTION_PREFIX", "ultrastonk-");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long parallelism = positive("FLINK_PARALLELISM", 2);
        if (parallelism > 128) throw new IllegalArgumentException("FLINK_PARALLELISM must be <= 128");
        env.setParallelism((int) parallelism);
        env.setMaxParallelism(128);
        if (Boolean.parseBoolean(setting("PROFILE_ISOLATE_OPERATORS", "false"))) env.disableOperatorChaining();
        env.enableCheckpointing(positive("CHECKPOINT_INTERVAL_MS", 10000), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Duration.ofSeconds(10)));

        boolean boundedReplay = Boolean.parseBoolean(setting("FLINK_BOUNDED_REPLAY", "false"));
        var sourceBuilder = KafkaSource.<PriceInput>builder()
                .setBootstrapServers(brokers)
                .setTopics(setting("KAFKA_INPUT_TOPIC", "price-events"))
                .setGroupId(setting("KAFKA_GROUP_ID", "ultrastonk-flink"))
                // Cold start must rebuild indicators from retained history, not just group offsets.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("isolation.level", "read_committed")
                .setProperty("partition.discovery.interval.ms", "10000")
                .setValueOnlyDeserializer(new PriceValueSchema());
        if (boundedReplay) sourceBuilder.setBounded(OffsetsInitializer.latest());
        var watermarks = WatermarkStrategy.<PriceInput>forBoundedOutOfOrderness(
                        Duration.ofMillis(nonnegative("MAX_OUT_OF_ORDER_MS", 5000)))
                .withTimestampAssigner((input, kafkaTimestamp) -> input.eventTimeMs);
        // The Kafka source tracks progress per split. For a finite read, do not treat quiet
        // partitions as idle: their known stopping offsets define completion instead.
        if (!boundedReplay) watermarks = watermarks.withIdleness(
                Duration.ofMillis(positive("SOURCE_IDLE_TIMEOUT_MS", 30000)));
        var parsed = env.fromSource(sourceBuilder.build(), watermarks, "Kafka prices").uid("kafka-prices")
                .process(new ParsePriceEvent()).name("Route prices and rejections").uid("validate-prices");
        var indicators = parsed.keyBy(event -> event.ticker)
                .process(new PriceIndicators(positive("WINDOW_SIZE_MS", 300000), policies))
                .name("Price and window EMAs").uid("price-indicators");

        indicators.sinkTo(KafkaSink.<EventProcessed>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(new ProcessedSerializer(setting("KAFKA_OUTPUT_TOPIC", "events-processed")))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(prefix + "processed-")
                .setProperty("transaction.timeout.ms", "900000")
                .build()).name("Kafka processed events").uid("kafka-processed");

        parsed.getSideOutput(ParsePriceEvent.REJECTED).union(indicators.getSideOutput(PriceIndicators.LATE))
                .sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(setting("KAFKA_REJECTED_TOPIC", "price-events-rejected"))
                        .setValueSerializationSchema(new RejectedSerializer()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(prefix + "rejected-")
                .setProperty("transaction.timeout.ms", "900000")
                .build()).name("Kafka rejected events").uid("kafka-rejected");
        env.execute("Ultrastonk price indicators");
    }

    private static String setting(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static long positive(String name, long fallback) {
        long value = nonnegative(name, fallback);
        if (value == 0) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }

    private static long nonnegative(String name, long fallback) {
        long value = Long.parseLong(setting(name, Long.toString(fallback)));
        if (value < 0) throw new IllegalArgumentException(name + " must be nonnegative");
        return value;
    }

    static final class ProcessedSerializer implements KafkaRecordSerializationSchema<EventProcessed> {
        private final String topic;
        private transient RollingHistogram ingressToSinkMs;
        private transient RollingHistogram emitToSinkMs;
        private transient ComponentProfiler profiler;
        ProcessedSerializer(String topic) { this.topic = topic; }

        @Override public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
            var metrics = context.getMetricGroup().addGroup("pipeline");
            ingressToSinkMs = metrics.histogram("ingressToSinkMs", new RollingHistogram());
            emitToSinkMs = metrics.histogram("emitToSinkMs", new RollingHistogram());
            profiler = new ComponentProfiler(context.getMetricGroup(), "processed_serialization");
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(EventProcessed event, KafkaSinkContext context, Long timestamp) {
            if (event.type.equals("price")) {
                long now = System.currentTimeMillis();
                if (event.ingestionTimeMs > 0) ingressToSinkMs.update(Math.max(0, now - event.ingestionTimeMs));
                if (now >= event.timestamp_emit) emitToSinkMs.update(now - event.timestamp_emit);
            }
            long started = profiler.begin();
            try {
                return new ProducerRecord<>(topic, event.ticker.getBytes(StandardCharsets.UTF_8),
                        EventJson.write(event).getBytes(StandardCharsets.UTF_8));
            } finally {
                profiler.end(started);
            }
        }
    }

    static final class RejectedSerializer extends SimpleStringSchema {
        private transient ComponentProfiler profiler;
        @Override public void open(SerializationSchema.InitializationContext context) {
            profiler = new ComponentProfiler(context.getMetricGroup(), "rejected_serialization");
        }
        @Override public byte[] serialize(String value) {
            long started = profiler.begin();
            try { return super.serialize(value); }
            finally { profiler.end(started); }
        }
    }
}
