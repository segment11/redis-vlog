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
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkPwrite {
    final String filePath = "/tmp/test_jnr_libc_pwrite";

    // 16 is best
    /*
Benchmark               (batchPages)   Mode  Cnt    Score   Error   Units
BenchmarkPwrite.pwrite             1  thrpt       104.146          ops/ms
BenchmarkPwrite.pwrite             4  thrpt        77.399          ops/ms
BenchmarkPwrite.pwrite            16  thrpt        28.831          ops/ms
BenchmarkPwrite.pwrite            64  thrpt        11.175          ops/ms
BenchmarkPwrite.pwrite           256  thrpt         3.448          ops/ms
BenchmarkPwrite.pwrite             1   avgt         0.010           ms/op
BenchmarkPwrite.pwrite             4   avgt         0.013           ms/op
BenchmarkPwrite.pwrite            16   avgt         0.033           ms/op
BenchmarkPwrite.pwrite            64   avgt         0.086           ms/op
BenchmarkPwrite.pwrite           256   avgt         0.285           ms/op
     */
    @Param({"1", "4", "16", "64", "256"})
    int batchPages = 1;

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

        long writeN = 0;
        for (int i = 0; i < maxBatches; i++) {
            buf.putInt(i);
            buf.rewind();
            writeN += libC.pwrite(fd, buf, buf.capacity(), i * pageSize * batchPages);
            buf.position(0);
        }
        System.out.printf("init write done, batches: %d\n", writeN / pageSize / batchPages);
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
