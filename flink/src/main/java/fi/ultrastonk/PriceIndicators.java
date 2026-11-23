package fi.ultrastonk;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One closing tick per open window and two EMA values per ticker, all checkpointed. */
public class PriceIndicators extends KeyedProcessFunction<String, PriceEvent, EventProcessed> {
    private static final Logger LOG = LoggerFactory.getLogger(PriceIndicators.class);
    public static final OutputTag<String> LATE = new OutputTag<String>("late-price-events") {};
    private final long windowSizeMs;
    private final Policies policies;
    private transient MapState<Long, PriceEvent> closes;
    private transient MapState<String, Boolean> seen;
    private transient ValueState<Double> ema38;
    private transient ValueState<Double> ema100;
    private transient ValueState<Long> finalizedThrough;
    private transient Counter lateEvents;
    private transient Counter duplicateEvents;
    private transient Counter acceptedEvents;
    private transient Counter emittedEvents;
    private transient Counter completedWindows;
    private transient Counter graceEvents;
    private transient RollingHistogram operatorMicros;
    private transient RollingHistogram lateByMs;
    private transient long nextLogTime;
    private transient long watermark;
    private transient ComponentProfiler dedupProfile;
    private transient ComponentProfiler windowProfile;
    private transient ComponentProfiler closeProfile;
    private transient ComponentProfiler ema38Profile;
    private transient ComponentProfiler ema100Profile;

    public PriceIndicators(long windowSizeMs) {
        this(windowSizeMs, Policies.defaults());
    }

    public PriceIndicators(long windowSizeMs, Policies policies) {
        if (windowSizeMs <= 0 || windowSizeMs > 86_400_000L) {
            throw new IllegalArgumentException("WINDOW_SIZE_MS must be in [1, 86400000]");
        }
        this.windowSizeMs = windowSizeMs;
        this.policies = policies;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        closes = getRuntimeContext().getMapState(new MapStateDescriptor<>("window-closes", Long.class, PriceEvent.class));
        ema38 = getRuntimeContext().getState(new ValueStateDescriptor<>("ema38", Double.class));
        ema100 = getRuntimeContext().getState(new ValueStateDescriptor<>("ema100", Double.class));
        finalizedThrough = getRuntimeContext().getState(new ValueStateDescriptor<>("finalized-through", Long.class));
        if (policies.deduplication != Policies.Deduplication.NONE) {
            var descriptor = new MapStateDescriptor<>("dedup-" + policies.deduplication.name(), String.class, Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds(policies.dedupTtlMs))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupIncrementally(100, true)
                    .cleanupInRocksdbCompactFilter(1000).build());
            seen = getRuntimeContext().getMapState(descriptor);
        }
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("pipeline");
        lateEvents = metrics.counter("latePriceEvents");
        duplicateEvents = metrics.counter("duplicateEvents");
        acceptedEvents = metrics.counter("acceptedEvents");
        emittedEvents = metrics.counter("emittedEvents");
        completedWindows = metrics.counter("completedWindows");
        graceEvents = metrics.counter("graceAcceptedEvents");
        operatorMicros = metrics.histogram("operatorMicros", new RollingHistogram());
        lateByMs = metrics.histogram("lateByMs", new RollingHistogram());
        metrics.gauge("watermark", () -> watermark);
        var group = getRuntimeContext().getMetricGroup();
        dedupProfile = new ComponentProfiler(group, "deduplication");
        windowProfile = new ComponentProfiler(group, "window_state");
        closeProfile = new ComponentProfiler(group, "window_close_state");
        ema38Profile = new ComponentProfiler(group, "ema38");
        ema100Profile = new ComponentProfiler(group, "ema100");
        LOG.info("event=policy_config dedup={} late={} window_ms={} grace_ms={} dedup_ttl_ms={}",
                policies.deduplication, policies.lateEvents, windowSizeMs, policies.graceMs, policies.dedupTtlMs);
    }

    @Override
    public void processElement(PriceEvent event, Context ctx, Collector<EventProcessed> out) throws Exception {
        long started = System.nanoTime();
        try {
            process(event, ctx, out);
        } finally {
            operatorMicros.update((System.nanoTime() - started) / 1000);
            long now = System.currentTimeMillis();
            if (now >= nextLogTime) {
                nextLogTime = now + policies.logIntervalMs;
                LOG.info("event=pipeline_stats accepted={} duplicates={} late={} grace_accepted={} windows={} emitted={} watermark={} operator_p99_us={}",
                        acceptedEvents.getCount(), duplicateEvents.getCount(), lateEvents.getCount(), graceEvents.getCount(),
                        completedWindows.getCount(), emittedEvents.getCount(), watermark, operatorMicros.getStatistics().getQuantile(0.99));
            }
        }
    }

    private void process(PriceEvent event, Context ctx, Collector<EventProcessed> out) throws Exception {
        watermark = ctx.timerService().currentWatermark();
        if (seen != null && duplicate(event)) {
            duplicateEvents.inc();
            return;
        }
        acceptedEvents.inc();
        emit(event, "price", event.price, out);
        boolean audit;
        long started = windowProfile.begin();
        try {
            audit = updateWindow(event, ctx);
        } finally {
            windowProfile.end(started);
        }
        if (audit) ctx.output(LATE, EventJson.rejected(EventJson.write(event), "window already finalized"));
    }

    private boolean duplicate(PriceEvent event) throws Exception {
        long started = dedupProfile.begin();
        try {
            String id = Long.toString(event.timestamp_emit);
            if (policies.deduplication == Policies.Deduplication.EXACT) {
                id += ":" + Long.toHexString(Double.doubleToLongBits(event.price)) + ":" + event.sec_type;
            }
            if (seen.contains(id)) {
                return true;
            }
            seen.put(id, true);
            return false;
        } finally {
            dedupProfile.end(started);
        }
    }

    private boolean updateWindow(PriceEvent event, Context ctx) throws Exception {
        long windowEnd = (event.timestamp_emit / windowSizeMs + 1) * windowSizeMs;
        long nominalTimer = windowEnd - 1;
        long timer = nominalTimer + (policies.lateEvents == Policies.LateEvents.GRACE ? policies.graceMs : 0);
        // Watermarks themselves can reset during recovery. Never reopen a window
        // whose EMA was already included in checkpointed state.
        Long finalized = finalizedThrough.value();
        long effectiveWatermark = finalized == null ? watermark : Math.max(watermark, finalized);
        if (nominalTimer <= watermark) {
            lateByMs.update(watermark - nominalTimer);
        }
        if (timer <= effectiveWatermark) {
            // Keep the raw-price feed complete; finalized EMA windows are immutable.
            lateEvents.inc();
            return policies.lateEvents != Policies.LateEvents.DROP;
        }
        if (nominalTimer <= watermark) graceEvents.inc();
        PriceEvent previous = closes.get(timer);
        if (previous == null || event.timestamp_emit >= previous.timestamp_emit) {
            // Equal timestamps: last record received for the key wins.
            closes.put(timer, event);
        }
        ctx.timerService().registerEventTimeTimer(timer);
        return false;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EventProcessed> out) throws Exception {
        PriceEvent close;
        long started = closeProfile.begin();
        try {
            close = closes.get(timestamp);
            if (close != null) {
                finalizedThrough.update(timestamp);
                closes.remove(timestamp);
            }
        } finally {
            closeProfile.end(started);
        }
        if (close == null) return;
        double shortValue = updateEma(close.price, ema38, 38, ema38Profile);
        double longValue = updateEma(close.price, ema100, 100, ema100Profile);
        emit(close, "EMA38", shortValue, out);
        emit(close, "EMA100", longValue, out);
        completedWindows.inc();
    }

    private double updateEma(double close, ValueState<Double> state, int period, ComponentProfiler profiler) throws Exception {
        long started = profiler.begin();
        try {
            double value = next(close, state.value(), period);
            state.update(value);
            return value;
        } finally {
            profiler.end(started);
        }
    }

    private void emit(PriceEvent source, String type, double value, Collector<EventProcessed> out) {
        EventProcessed event = new EventProcessed(source.timestamp_emit, source.ticker, type, value);
        event.ingestionTimeMs = source.ingestionTimeMs;
        out.collect(event);
        emittedEvents.inc();
    }

    private static double next(double close, Double previous, int period) {
        double alpha = 2.0 / (period + 1);
        return alpha * close + (1 - alpha) * (previous == null ? 0 : previous);
    }
}
