package redis;

import redis.stats.OfStats;
import redis.stats.StatKV;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CompressStats implements OfStats {
    public long rawCount = 0;
    public long compressedCount = 0;
    public long rawValueBodyTotalLength = 0;
    public long compressedValueBodyTotalLength = 0;
    public long compressedCostTotalTimeNanos = 0;
    public long decompressedCount = 0;
    public long decompressedCostTotalTimeNanos = 0;

    // for segment cache, multi-thread
    public long decompressCount2 = 0;
    public long decompressCostTotalTimeNanos2 = 0;
    public long compressedValueBodyTotalLength2 = 0;
    public long compressedValueSizeTotalCount2 = 0;
    public long rawValueBodyTotalLength2 = 0;

    // for key tmp bucket size
    private final ConcurrentMap<Integer, Short> keySizeByBucket = new ConcurrentHashMap<>();

    public long getAllTmpBucketSize() {
        long sum = 0;
        for (var value : keySizeByBucket.values()) {
            sum += value;
        }
        return sum;
    }

    // bucket index max is 4096
    public void updateTmpBucketSize(byte slot, int bucketIndex, byte splitIndex, short size) {
        int key = slot << (Short.SIZE + 4) | bucketIndex << Byte.SIZE | splitIndex;
        keySizeByBucket.put(key, size);
    }

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

        if (decompressCount2 > 0) {
            list.add(new StatKV(prefix + "decompress count2", decompressCount2));
            list.add(new StatKV(prefix + "decompress cost total millis2", (double) decompressCostTotalTimeNanos2 / 1000 / 1000));
            double decompressCostTAvg2 = (double) decompressCostTotalTimeNanos2 / decompressCount2 / 1000;
            list.add(new StatKV(prefix + "decompress cost avg micros2", decompressCostTAvg2));

            if (rawValueBodyTotalLength2 > 0) {
                list.add(new StatKV(prefix + "raw total length2", rawValueBodyTotalLength2));
                list.add(new StatKV(prefix + "compressed total length2", compressedValueBodyTotalLength2));
                list.add(new StatKV(prefix + "compression ratio2", (double) compressedValueBodyTotalLength2 / rawValueBodyTotalLength2));
            }

            if (compressedValueSizeTotalCount2 > 0) {
                list.add(new StatKV(prefix + "compressed bucket key size total count2", compressedValueSizeTotalCount2));
                list.add(new StatKV(prefix + "compressed bucket key length total2", compressedValueBodyTotalLength2));
                list.add(new StatKV(prefix + "compressed bucket key length avg2", (double) compressedValueBodyTotalLength2 / compressedValueSizeTotalCount2));
            }
        }

        list.add(StatKV.split);
        return list;
    }
}
