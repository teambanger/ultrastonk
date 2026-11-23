package fi.ultrastonk;

import java.util.Arrays;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/** Bounded rolling sample; Prometheus quantiles describe the last 2048 observations per task. */
public final class RollingHistogram implements Histogram {
    private final long[] samples = new long[2048];
    private long count;

    @Override public synchronized void update(long value) { samples[(int) (count++ % samples.length)] = value; }
    @Override public synchronized long getCount() { return count; }
    @Override public synchronized HistogramStatistics getStatistics() {
        long[] values = Arrays.copyOf(samples, (int) Math.min(count, samples.length));
        Arrays.sort(values);
        return new HistogramStatistics() {
            @Override public double getQuantile(double quantile) {
                if (values.length == 0) return 0;
                double index = Math.max(0, Math.min(1, quantile)) * (values.length - 1);
                int lower = (int) index;
                int upper = Math.min(values.length - 1, lower + 1);
                return values[lower] + (index - lower) * (values[upper] - values[lower]);
            }
            @Override public long[] getValues() { return values.clone(); }
            @Override public int size() { return values.length; }
            @Override public double getMean() { return Arrays.stream(values).average().orElse(0); }
            @Override public double getStdDev() {
                double mean = getMean();
                return Math.sqrt(Arrays.stream(values).mapToDouble(v -> (v - mean) * (v - mean)).average().orElse(0));
            }
            @Override public long getMax() { return values.length == 0 ? 0 : values[values.length - 1]; }
            @Override public long getMin() { return values.length == 0 ? 0 : values[0]; }
        };
    }
}
