package redis.jmh;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static redis.persist.LocalPersist.O_DIRECT;
import static redis.persist.LocalPersist.PROTECTION;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 1, time = 5)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkPwrite {
    final String filePath = "/tmp/test_jnr_libc_pwrite";

    // 16 is best
    /*
Benchmark               (batchPages)   Mode  Cnt   Score   Error   Units
BenchmarkPwrite.pwrite             4  thrpt       73.812          ops/ms
BenchmarkPwrite.pwrite            16  thrpt       37.561          ops/ms
BenchmarkPwrite.pwrite            64  thrpt        8.205          ops/ms
BenchmarkPwrite.pwrite           256  thrpt        2.050          ops/ms
BenchmarkPwrite.pwrite             4   avgt        0.015           ms/op
BenchmarkPwrite.pwrite            16   avgt        0.030           ms/op
BenchmarkPwrite.pwrite            64   avgt        0.123           ms/op
BenchmarkPwrite.pwrite           256   avgt        0.480           ms/op
     */
    @Param({"4", "16", "64", "256"})
    int batchPages = 4;

    long addr;

    ByteBuffer buf;

    LibC libC;

    int fd;

    long pageSize;

    final int maxBatches = 1024;

    @Setup
    public void setup() throws IOException {
        System.setProperty("jnr.ffi.asm.enabled", "false");
        libC = LibraryLoader.create(LibC.class).load("c");

        var file = new File(filePath);
        FileUtils.touch(file);
        fd = libC.open(filePath, O_DIRECT | OpenFlags.O_WRONLY.value(), 00644);

        var m = MemoryIO.getInstance();
        var pageManager = PageManager.getInstance();

        pageSize = pageManager.pageSize();
        addr = pageManager.allocatePages(batchPages, PROTECTION);
        buf = m.newDirectByteBuffer(addr, (int) pageSize * batchPages);

        System.out.printf("init buf capacity: %d, addr: %d\n", buf.capacity(), addr);
    }

    @TearDown
    public void tearDown() {
        if (addr != 0) {
            PageManager.getInstance().freePages(addr, batchPages);
        }
        if (fd != 0) {
            libC.close(fd);
        }
    }

    private final Random random = new Random();

    @Benchmark
    public void pwrite() {
        int i = random.nextInt(maxBatches);
        libC.pwrite(fd, buf, buf.capacity(), i * pageSize * batchPages);
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkPwrite.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
