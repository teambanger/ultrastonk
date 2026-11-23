package fi.ultrastonk;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ComponentProfilerTest {
    static class Metrics extends UnregisteredMetricsGroup {
        final Map<String, Counter> counters = new HashMap<>();
        @Override public MetricGroup addGroup(String name) { return this; }
        @Override public MetricGroup addGroup(String key, String value) { return this; }
        @Override public Counter counter(String name) { return counters.computeIfAbsent(name, key -> new SimpleCounter()); }
        long count(String name) { return counters.get(name).getCount(); }
    }
    static class Clock implements ComponentProfiler.Clock {
        long wall = 100, cpu = 100, allocated = 100;
        public long wall() { return wall; }
        public long cpu() { return cpu; }
        public long allocated() { return allocated; }
        void advance() { wall += 2000; cpu += 1000; allocated += 32; }
    }

    @Test void samplesCallsWithoutScalingCountersOrDoubleCounting() {
        var metrics = new Metrics();
        var clock = new Clock();
        var profiler = new ComponentProfiler(metrics, "test", true, 3, clock);
        for (int i = 0; i < 7; i++) {
            long start = profiler.begin();
            clock.advance();
            profiler.end(start);
        }
        assertEquals(7, metrics.count("calls"));
        assertEquals(3, metrics.count("samples"));
        assertEquals(6000, metrics.count("wallNanos"));
        assertEquals(3000, metrics.count("cpuNanos"));
        assertEquals(96, metrics.count("allocatedBytes"));
    }

    @Test void unsupportedMeasurementsStayUnavailableAndDisabledDoesNoWork() {
        var metrics = new Metrics();
        var clock = new Clock();
        clock.cpu = -1;
        clock.allocated = -1;
        var profiler = new ComponentProfiler(metrics, "test", true, 1, clock);
        long start = profiler.begin();
        clock.wall += 100;
        profiler.end(start);
        assertEquals(1, metrics.count("samples"));
        assertEquals(0, metrics.count("cpuSamples"));
        assertEquals(0, metrics.count("allocationSamples"));
        profiler = new ComponentProfiler(metrics, "test", false, 1, clock);
        assertEquals(-1, profiler.begin());
        assertEquals(1, metrics.count("calls"));
    }
}
