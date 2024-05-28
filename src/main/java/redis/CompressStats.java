package redis;

import redis.metric.SimpleGauge;

import java.util.HashMap;
import java.util.List;

public class CompressStats {
    public long rawCount = 0;
    public long compressedCount = 0;
    public long rawTotalLength = 0;
    public long compressedTotalLength = 0;
    public long compressedCostTimeTotalUs = 0;
    public long decompressedCount = 0;
    public long decompressedCostTimeTotalUs = 0;

    private final String name;

    public CompressStats(String name) {
        this.name = name;

        compressStatsGauge.addRawGetter(() -> {
            var labelValues = List.of(name);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            if (compressedCount > 0) {
                map.put("raw_count", new SimpleGauge.ValueWithLabelValues((double) rawCount, labelValues));
                map.put("compressed_count", new SimpleGauge.ValueWithLabelValues((double) compressedCount, labelValues));
                map.put("compressed_cost_time_total_ms", new SimpleGauge.ValueWithLabelValues((double) compressedCostTimeTotalUs / 1000, labelValues));
                double costTAvg = (double) compressedCostTimeTotalUs / compressedCount;
                map.put("compressed_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues(costTAvg, labelValues));

                map.put("raw_total_length", new SimpleGauge.ValueWithLabelValues((double) rawTotalLength, labelValues));
                map.put("compressed_total_length", new SimpleGauge.ValueWithLabelValues((double) compressedTotalLength, labelValues));
                if (compressedTotalLength > 0) {
                    map.put("compression_ratio", new SimpleGauge.ValueWithLabelValues((double) compressedTotalLength / rawTotalLength, labelValues));
                }
            }

            if (decompressedCount > 0) {
                map.put("decompressed_count", new SimpleGauge.ValueWithLabelValues((double) decompressedCount, labelValues));
                map.put("decompressed_cost_time_total_ms", new SimpleGauge.ValueWithLabelValues((double) decompressedCostTimeTotalUs / 1000, labelValues));
                double decompressedCostTAvg = (double) decompressedCostTimeTotalUs / decompressedCount;
                map.put("decompressed_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues(decompressedCostTAvg, labelValues));
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
