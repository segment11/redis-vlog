package redis.jmh;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.SnowFlake;

import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 1, time = 5)
@State(Scope.Thread)
@Threads(2)
public class BenchmarkSnowFlake {
    private final SnowFlake snowFlake = new SnowFlake(0, 0);

    @Benchmark
    public void nextId() {
        snowFlake.nextId();
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkSnowFlake.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
