package redis.jmh;

import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.persist.FdReadWrite;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(4)
public class BenchmarkFdReadWrite {
    final String dirPath = "/tmp/test_fd_read_write_jmh";

    @Param({"1"})
    int fileNumber = 1;

    final int maxSegmentCount = 1024 * 1024 / 2;

    LibC libC;

    ArrayList<FdReadWrite> fdReadWriteList = new ArrayList<>();

    @Setup
    public void setup() throws IOException {
        System.setProperty("jnr.ffi.asm.enabled", "false");
        libC = LibraryLoader.create(LibC.class).load("c");

        var dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        var threadId = Thread.currentThread().getId();
        var targetDir = new File(dir, "thread_" + threadId);
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }

        var threadFactory = new AffinityThreadFactory("fd-read-write-group-1", AffinityStrategies.SAME_CORE);

        // 2M
        var bytes = new byte[1024 * 1024 * 2];
        var buffer = ByteBuffer.wrap(bytes);
        var pageSize = 4096;
        var pageNumber = bytes.length / pageSize;
        for (int i = 0; i < pageNumber; i++) {
            buffer.position(i * pageSize).putInt(i);
        }

        for (int i = 0; i < fileNumber; i++) {
            var file = new File(targetDir, "test_fd_read_write_jmh_" + i);
            if (!file.exists()) {
                FileUtils.touch(file);

                final int maxBatches = 1024;
                long writeN = 0;
                for (int j = 0; j < maxBatches; j++) {
                    FileUtils.writeByteArrayToFile(file, bytes, true);
                    writeN += bytes.length;
                }
                System.out.printf("init write done, size: %d GB\n", writeN / 1024 / 1024);
            } else {
                System.out.printf("file exists, size: %d GB\n", file.length() / 1024 / 1024);
            }

            var fdReadWrite = new FdReadWrite("test" + i, libC, file);
            fdReadWrite.initByteBuffers(false);
            fdReadWrite.initEventloop(threadFactory);
            fdReadWriteList.add(fdReadWrite);
        }
    }

    @TearDown
    public void tearDown() {
        for (var fdReadWrite : fdReadWriteList) {
            fdReadWrite.cleanUp();
        }

        System.out.println("Init int value set: " + initIntValueSet);
    }

    private final Random random = new Random();

    private Set<Integer> initIntValueSet = new HashSet<>();

    /*
    Threads: 4
Benchmark                   (fileNumber)   Mode  Cnt    Score   Error   Units
BenchmarkFdReadWrite.read              1  thrpt        32.542          ops/ms
BenchmarkFdReadWrite.write             1  thrpt       177.491          ops/ms
BenchmarkFdReadWrite.read              1   avgt         0.052           ms/op
BenchmarkFdReadWrite.write             1   avgt         0.022           ms/op
     */

    /*
    Threads: 1
Benchmark                  (fileNumber)   Mode  Cnt   Score   Error   Units
BenchmarkFdReadWrite.read             1  thrpt       20.702          ops/ms
BenchmarkFdReadWrite.read             1   avgt        0.049           ms/op
     */
    @Benchmark
    public void read() {
        int segmentIndex = random.nextInt(maxSegmentCount);
        try {
            for (var fdReadWrite : fdReadWriteList) {
                var f = fdReadWrite.readSegment(segmentIndex * 4096L, false);
                var bytes = f.get();
                var intValueInit = ByteBuffer.wrap(bytes).getInt();
                initIntValueSet.add(intValueInit);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private final byte[] writeBytes = new byte[4096];

    /*
    Threads: 1
Benchmark                   (fileNumber)   Mode  Cnt   Score   Error   Units
BenchmarkFdReadWrite.write             1  thrpt       47.095          ops/ms
BenchmarkFdReadWrite.write             1   avgt        0.020           ms/op
     */
    @Benchmark
    public void write() {
        int segmentIndex = random.nextInt(maxSegmentCount);
        try {
            for (var fdReadWrite : fdReadWriteList) {
                var f = fdReadWrite.writeSegment(segmentIndex * 4096L, writeBytes, false);
                var n = f.get();
                if (n != 4096) {
                    throw new RuntimeException("write failed");
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkFdReadWrite.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
