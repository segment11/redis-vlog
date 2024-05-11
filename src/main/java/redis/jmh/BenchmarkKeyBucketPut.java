package redis.jmh;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.CompressStats;
import redis.ConfForSlot;
import redis.KeyHash;
import redis.SnowFlake;
import redis.persist.KeyBucket;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkKeyBucketPut {
    private String[] keys;
    private Long[] keysHash;

    private SnowFlake snowFlake;

    @Param({"10000", "100000"})
    int size = 10000;

    @Setup
    public void setup() {
        keys = new String[size];
        keysHash = new Long[size];
        for (int i = 0; i < size; i++) {
            keys[i] = UUID.randomUUID().toString();
            keysHash[i] = KeyHash.hash(keys[i].getBytes());
        }
        System.out.printf("init keys / keys hash, size: %d\n", size);

        snowFlake = new SnowFlake(1, 1);
        ConfForSlot.global.confBucket.isCompress = false;
    }

    /*
Benchmark                        (size)  Mode  Cnt   Score   Error  Units
BenchmarkKeyBucketPut.put         10000  avgt        2.444          ms/op
BenchmarkKeyBucketPut.put        100000  avgt       24.472          ms/op
BenchmarkKeyBucketPut.putAndGet   10000  avgt        2.450          ms/op
BenchmarkKeyBucketPut.putAndGet  100000  avgt       24.527          ms/op
     */

    @Benchmark
    public void put() {
        final int capacity = 50;
        final byte[] valueBytes = "value-test".getBytes();
        final CompressStats compressStats = new CompressStats("test");

        KeyBucket keyBucket = null;
        for (int i = 0; i < size; i++) {
            if (i % capacity == 0) {
                keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake);
                keyBucket.initWithCompressStats(compressStats);
            }

            var key = keys[i];
            var keyHash = keysHash[i];
            keyBucket.put(key.getBytes(), keyHash, 0L, valueBytes, null);
        }
    }

    @Benchmark
    public void putAndGet(){
        final int capacity = 50;
        final byte[] valueBytes = "value-test".getBytes();
        final CompressStats compressStats = new CompressStats("test");

        KeyBucket keyBucket = null;
        for (int i = 0; i < size; i++) {
            if (i % capacity == 0) {
                keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake);
                keyBucket.initWithCompressStats(compressStats);
            }

            var key = keys[i];
            var keyHash = keysHash[i];
            keyBucket.put(key.getBytes(), keyHash, 0L, valueBytes, null);
            keyBucket.getValueByKey(key.getBytes(), keyHash);
        }
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkKeyBucketPut.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();

//        var x = new BenchmarkKeyBucketPut();
//        x.size = 100;
//        x.setup();
//
//        var beginT = System.currentTimeMillis();
//        x.putAndGet();
//        var endT = System.currentTimeMillis();
//        System.out.printf("time cost: %d ms\n", endT - beginT);
    }
}
