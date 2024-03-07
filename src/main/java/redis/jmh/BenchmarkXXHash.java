package redis.jmh;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 1, time = 5)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkXXHash {
    private String[] keys;

    @Param({"1000000", "10000000"})
    int size = 1_000_000;

    @Setup
    public void setup() {
        keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = UUID.randomUUID().toString();
        }
        System.out.printf("init keys, size: %d\n", size);
    }

    private final Random random = new Random();

    private final XXHash32 xxHash32 = XXHashFactory.nativeInstance().hash32();
    private final XXHash64 xxHash64 = XXHashFactory.nativeInstance().hash64();
    private final XXHash32 xxHash32Java = XXHashFactory.fastestJavaInstance().hash32();
    private final XXHash64 xxHash64Java = XXHashFactory.fastestJavaInstance().hash64();

    /*
Benchmark                        (size)   Mode  Cnt  Score   Error   Units
BenchmarkXXHashJNI.hash32       1000000  thrpt       8.011          ops/us
BenchmarkXXHashJNI.hash32      10000000  thrpt       5.167          ops/us
BenchmarkXXHashJNI.hash32Java   1000000  thrpt       9.169          ops/us
BenchmarkXXHashJNI.hash32Java  10000000  thrpt       5.447          ops/us
BenchmarkXXHashJNI.hash32       1000000   avgt       0.120           us/op
BenchmarkXXHashJNI.hash32      10000000   avgt       0.195           us/op
BenchmarkXXHashJNI.hash32Java   1000000   avgt       0.111           us/op
BenchmarkXXHashJNI.hash32Java  10000000   avgt       0.186           us/op
     */

    @Benchmark
    public void hash32() {
        var key = keys[random.nextInt(size)];
        xxHash32.hash(key.getBytes(), 0, key.length(), 0);
    }

    @Benchmark
    public void hash32Java() {
        var key = keys[random.nextInt(size)];
        xxHash32Java.hash(key.getBytes(), 0, key.length(), 0);
    }

    @Benchmark
    public void hash64() {
        var key = keys[random.nextInt(size)];
        xxHash64.hash(key.getBytes(), 0, key.length(), 0);
    }

    @Benchmark
    public void hash64Java() {
        var key = keys[random.nextInt(size)];
        xxHash64Java.hash(key.getBytes(), 0, key.length(), 0);
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkXXHash.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
