package redis;

import redis.stats.OfStats;
import redis.stats.StatKV;

import java.util.ArrayList;
import java.util.List;

public class CompressStats implements OfStats {
    public long rawCount = 0;
    public long compressedCount = 0;
    public long rawValueBodyTotalLength = 0;
    public long compressedValueBodyTotalLength = 0;
    public long compressedCostTotalTimeNanos = 0;
    public long decompressedCount = 0;
    public long decompressedCostTotalTimeNanos = 0;

    private final String prefix;

    public CompressStats(String prefix) {
        this.prefix = prefix.endsWith(" ") ? prefix : prefix + " ";
    }

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();

        if (compressedCount > 0) {
            list.add(new StatKV(prefix + "raw count", rawCount));
            list.add(new StatKV(prefix + "compressed count", compressedCount));
            list.add(new StatKV(prefix + "compressed cost total millis", (double) compressedCostTotalTimeNanos / 1000 / 1000));
            double costTAvg = (double) compressedCostTotalTimeNanos / compressedCount / 1000;
            list.add(new StatKV(prefix + "compressed cost avg micros", costTAvg));

            long sum = rawValueBodyTotalLength;
            long sum1 = compressedValueBodyTotalLength;
            list.add(new StatKV(prefix + "raw total length", sum));
            list.add(new StatKV(prefix + "compressed total length", sum1));
            list.add(new StatKV(prefix + "compression ratio", (double) sum1 / sum));
        }

        if (decompressedCount > 0) {
            list.add(new StatKV(prefix + "decompressed count", decompressedCount));
            list.add(new StatKV(prefix + "decompressed cost total millis", (double) decompressedCostTotalTimeNanos / 1000 / 1000));
            double decompressedCostTAvg = (double) decompressedCostTotalTimeNanos / decompressedCount / 1000;
            list.add(new StatKV(prefix + "decompressed cost avg micros", decompressedCostTAvg));
        }

        list.add(StatKV.split);
        return list;
    }
}
