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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Thread)
@Threads(1)
public class BenchmarkPread {
    final String filePath = "/tmp/test_jnr_libc_pread";

    /*
Benchmark             (batchPages)   Mode  Cnt  Score   Error   Units
BenchmarkPread.pread             1  thrpt       6.490          ops/us
BenchmarkPread.pread             4  thrpt       6.478          ops/us
BenchmarkPread.pread            16  thrpt       6.565          ops/us
BenchmarkPread.pread            64  thrpt       6.516          ops/us
BenchmarkPread.pread           256  thrpt       6.517          ops/us
BenchmarkPread.pread             1   avgt       0.155           us/op
BenchmarkPread.pread             4   avgt       0.154           us/op
BenchmarkPread.pread            16   avgt       0.153           us/op
BenchmarkPread.pread            64   avgt       0.153           us/op
BenchmarkPread.pread           256   avgt       0.153           us/op
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
    public void pread() {
        buf.flip();

        int i = random.nextInt(maxBatches);
        long n = libC.pread(fd, buf, buf.capacity(), i * pageSize * batchPages);
        assert n == buf.capacity();
        assert i == buf.getInt();
    }

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(BenchmarkPread.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
