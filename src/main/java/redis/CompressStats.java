package redis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class CompressStats implements OfStats {
    public long rawCount = 0;
    public long compressedCount = 0;
    public long rawValueBodyTotalLength = 0;
    public long compressedValueBodyTotalLength = 0;
    public long compressedCostTotalTimeNanos = 0;
    public long decompressedCount = 0;
    public long decompressedCostTotalTimeNanos = 0;

    // for segment cache, multi-thread
    public LongAdder decompressCount2 = new LongAdder();
    public LongAdder decompressCostTotalTimeNanos2 = new LongAdder();
    public LongAdder compressedValueBodyTotalLength2 = new LongAdder();
    public LongAdder compressedValueSizeTotalCount2 = new LongAdder();
    public LongAdder rawValueBodyTotalLength2 = new LongAdder();

    // for key tmp bucket size
    private final ConcurrentMap<Integer, Integer> keySizeByBucket = new ConcurrentHashMap<>();
    // for bucket key access analyze
    private Cache<Integer, Integer> keySizeByBucketLru;

    public void initKeySizeByBucketLru(int expireAfterWrite, int expireAfterAccess, int maximumSize) {
        keySizeByBucketLru = Caffeine.newBuilder()
                .expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
                .expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
                .maximumSize(maximumSize)
                .build();
    }

    public int getLruBucketSize() {
        return keySizeByBucketLru.asMap().size();
    }

    public long getAllLruBucketSize() {
        long sum = 0;
        for (var value : keySizeByBucketLru.asMap().values()) {
            sum += value;
        }
        return sum;
    }

    public long getAllTmpBucketSize() {
        long sum = 0;
        for (var value : keySizeByBucket.values()) {
            sum += value;
        }
        return sum;
    }

    // bucket index max is 4096
    public void updateTmpBucketSize(byte slot, int bucketIndex, byte splitIndex, int size) {
        int key = slot << (Short.SIZE + 4) | bucketIndex << Byte.SIZE | splitIndex;
        keySizeByBucket.put(key, size);
        if (keySizeByBucketLru != null) {
            keySizeByBucketLru.put(key, size);
        }
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

        long sum2 = decompressCount2.sum();
        if (sum2 > 0) {
            list.add(new StatKV(prefix + "decompress count2", sum2));
            list.add(new StatKV(prefix + "decompress cost total millis2", (double) decompressCostTotalTimeNanos2.sum() / 1000 / 1000));
            double decompressCostTAvg2 = (double) decompressCostTotalTimeNanos2.sum() / sum2 / 1000;
            list.add(new StatKV(prefix + "decompress cost avg micros2", decompressCostTAvg2));

            long sum3 = rawValueBodyTotalLength2.sum();
            if (sum3 > 0) {
                list.add(new StatKV(prefix + "raw total length2", sum3));
                long sum32 = compressedValueBodyTotalLength2.sum();
                list.add(new StatKV(prefix + "compressed total length2", sum32));
                list.add(new StatKV(prefix + "compression ratio2", (double) sum32 / sum3));
            }

            long sum4 = compressedValueSizeTotalCount2.sum();
            if (sum4 > 0) {
                list.add(new StatKV(prefix + "compressed bucket key size total count2", sum4));
                long sum42 = compressedValueBodyTotalLength2.sum();
                list.add(new StatKV(prefix + "compressed bucket key length total2", sum42));
                list.add(new StatKV(prefix + "compressed bucket key length avg2", (double) sum42 / sum4));
            }
        }

        list.add(StatKV.split);
        return list;
    }
}
