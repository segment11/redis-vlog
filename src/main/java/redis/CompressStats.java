package redis;

import redis.metric.SimpleGauge;

import java.util.HashMap;
import java.util.List;

public class CompressStats {
    public long compressRawCount = 0;
    public long compressedCount = 0;
    public long compressRawTotalLength = 0;
    public long compressedTotalLength = 0;
    public long compressedCostTotalNanos = 0;
    public long decompressedCount = 0;
    public long decompressedCostTotalNanos = 0;

    private final String name;

    public CompressStats(String name) {
        this.name = name;

        compressStatsGauge.addRawGetter(() -> {
            var labelValues = List.of(name);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            if (compressedCount > 0) {
                map.put("compress_raw_count", new SimpleGauge.ValueWithLabelValues((double) compressRawCount, labelValues));
                map.put("compressed_count", new SimpleGauge.ValueWithLabelValues((double) compressedCount, labelValues));
                map.put("compressed_cost_total_millis", new SimpleGauge.ValueWithLabelValues((double) compressedCostTotalNanos / 1000 / 1000, labelValues));
                double costTAvg = (double) compressedCostTotalNanos / compressedCount / 1000;
                map.put("compressed_cost_avg_micros", new SimpleGauge.ValueWithLabelValues(costTAvg, labelValues));

                map.put("compress_raw_total_length", new SimpleGauge.ValueWithLabelValues((double) compressRawTotalLength, labelValues));
                map.put("compressed_total_length", new SimpleGauge.ValueWithLabelValues((double) compressedTotalLength, labelValues));
                if (compressedTotalLength > 0) {
                    map.put("compression_ratio", new SimpleGauge.ValueWithLabelValues((double) compressedTotalLength / compressRawTotalLength, labelValues));
                }
            }

            if (decompressedCount > 0) {
                map.put("decompressed_count", new SimpleGauge.ValueWithLabelValues((double) decompressedCount, labelValues));
                map.put("decompressed_cost_total_millis", new SimpleGauge.ValueWithLabelValues((double) decompressedCostTotalNanos / 1000 / 1000, labelValues));
                double decompressedCostTAvg = (double) decompressedCostTotalNanos / decompressedCount / 1000;
                map.put("decompressed_cost_avg_micros", new SimpleGauge.ValueWithLabelValues(decompressedCostTAvg, labelValues));
            }

            return map;
        });
    }

    private final static SimpleGauge compressStatsGauge = new SimpleGauge("compress_stats", "compress stats",
            "name");

    static {
        compressStatsGauge.register();
    }
}
