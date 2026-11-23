package fi.ultrastonk;

import java.lang.management.ManagementFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** Sampled synchronous, non-overlapping scopes, owned by one Flink task thread. */
public final class ComponentProfiler {
    interface Clock {
        long wall();
        long cpu();
        long allocated();
    }

    private static final class JvmClock implements Clock {
        private final java.lang.management.ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        private final com.sun.management.ThreadMXBean allocations = threads instanceof com.sun.management.ThreadMXBean
                ? (com.sun.management.ThreadMXBean) threads : null;
        @Override public long wall() { return System.nanoTime(); }
        @Override public long cpu() {
            return threads.isCurrentThreadCpuTimeSupported() && threads.isThreadCpuTimeEnabled()
                    ? threads.getCurrentThreadCpuTime() : -1;
        }
        @Override public long allocated() {
            return allocations != null && allocations.isThreadAllocatedMemorySupported() && allocations.isThreadAllocatedMemoryEnabled()
                    ? allocations.getThreadAllocatedBytes(Thread.currentThread().getId()) : -1;
        }
    }

    private final boolean enabled;
    private final int every;
    private final Clock clock;
    private final Counter calls;
    private final Counter samples;
    private final Counter wallNanos;
    private final Counter cpuNanos;
    private final Counter cpuSamples;
    private final Counter allocatedBytes;
    private final Counter allocationSamples;
    private final RollingHistogram wallMicros;
    private final RollingHistogram cpuMicros;
    private long cpuStart;
    private long allocationStart;

    public ComponentProfiler(MetricGroup metrics, String component) {
        this(metrics, component, Boolean.parseBoolean(env("PROFILE_ENABLED", "true")),
                Integer.parseInt(env("PROFILE_SAMPLE_EVERY", "100")), new JvmClock());
    }

    ComponentProfiler(MetricGroup metrics, String component, boolean enabled, int every, Clock clock) {
        if (every < 1) throw new IllegalArgumentException("PROFILE_SAMPLE_EVERY must be positive");
        this.enabled = enabled;
        this.every = every;
        this.clock = clock;
        MetricGroup group = metrics.addGroup("profile").addGroup("component", component);
        calls = group.counter("calls");
        samples = group.counter("samples");
        wallNanos = group.counter("wallNanos");
        cpuNanos = group.counter("cpuNanos");
        cpuSamples = group.counter("cpuSamples");
        allocatedBytes = group.counter("allocatedBytes");
        allocationSamples = group.counter("allocationSamples");
        wallMicros = group.histogram("wallMicros", new RollingHistogram());
        cpuMicros = group.histogram("cpuMicros", new RollingHistogram());
        group.gauge("enabled", () -> enabled ? 1 : 0);
        group.gauge("sampleEvery", () -> every);
        group.gauge("cpuTimeAvailable", () -> clock.cpu() >= 0 ? 1 : 0);
        group.gauge("allocationAvailable", () -> clock.allocated() >= 0 ? 1 : 0);
    }

    public long begin() {
        if (!enabled) return -1;
        calls.inc();
        if ((calls.getCount() - 1) % every != 0) return -1;
        cpuStart = clock.cpu();
        allocationStart = clock.allocated();
        return clock.wall();
    }

    public void end(long start) {
        if (start == -1) return;
        long wall = Math.max(0, clock.wall() - start);
        long cpuEnd = clock.cpu();
        long allocationEnd = clock.allocated();
        wallNanos.inc(wall);
        wallMicros.update(wall / 1000);
        samples.inc();
        if (cpuStart >= 0 && cpuEnd >= cpuStart) {
            long cpu = cpuEnd - cpuStart;
            cpuNanos.inc(cpu);
            cpuMicros.update(cpu / 1000);
            cpuSamples.inc();
        }
        if (allocationStart >= 0 && allocationEnd >= allocationStart) {
            allocatedBytes.inc(allocationEnd - allocationStart);
            allocationSamples.inc();
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
