package redis.persist;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import io.activej.async.callback.AsyncComputation;
import io.activej.eventloop.Eventloop;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.repl.content.ToMasterExistsSegmentMeta;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

public class FdReadWrite implements OfStats {

    public FdReadWrite(String name, LibC libC, File oneFile) throws IOException {
        this.name = name;
        this.libC = libC;

        if (!oneFile.exists()) {
            FileUtils.touch(oneFile);
        }
        this.fd = libC.open(oneFile.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);
        log.info("Opened fd: {}, name: {}", fd, name);

        this.segmentBytesByIndex = new LRUMap<>(ConfForSlot.global.confChunk.lru.maxSize);
    }

    private final String name;

    private final LibC libC;

    private final int fd;

    private static final Summary readTimeSummary = Summary.build().name("pread_time").
            help("fd read time summary").
            labelNames("fd_name").
            quantile(0.5, 0.05).
            quantile(0.9, 0.01).
            quantile(0.99, 0.01).
            quantile(0.999, 0.001)
            .register();

    private static final Counter readBytesCounter = Counter.build().name("pread_bytes").
            help("fd read bytes").
            labelNames("fd_name")
            .register();

    private static final Summary writeTimeSummary = Summary.build().name("write_time").
            help("fd write time summary").
            labelNames("fd_name").
            quantile(0.5, 0.05).
            quantile(0.9, 0.01).
            quantile(0.99, 0.01).
            quantile(0.999, 0.001)
            .register();

    private static final Counter writeBytesCounter = Counter.build().name("pwrite_bytes").
            help("fd write bytes").
            labelNames("fd_name")
            .register();

    private Eventloop eventloop;

    private long threadId;

    private final int idleMillis = 10;

    private final Logger log = LoggerFactory.getLogger(FdReadWrite.class);

    public void initEventloop(ThreadFactory threadFactory) {
        var threadName = "fd-read-write-" + name;
        this.eventloop = Eventloop.builder()
                .withThreadName(threadName)
                .withIdleInterval(Duration.ofMillis(idleMillis))
                .build();
        this.eventloop.keepAlive(true);

        var thread = threadFactory.newThread(this.eventloop);
        thread.start();
        log.info("Init eventloop, thread name: {}", threadName);

        this.threadId = thread.threadId();
    }

    void stopEventLoop() {
        if (this.eventloop != null) {
            var threadName = "fd-read-write-" + name;
            this.eventloop.breakEventloop();
            System.out.println("Eventloop stopped, thread name: " + threadName);

            this.eventloop = null;
        }
    }

    // make sure in the same thread
    private <T> CompletableFuture<T> call(Callable<T> callable) {
        var currentThreadId = Thread.currentThread().getId();
        if (currentThreadId == threadId) {
            CompletableFuture<T> future = new CompletableFuture<>();
            try {
                future.complete(callable.call());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        } else {
            return eventloop.submit(AsyncComputation.of(callable::call));
        }
    }

    public static final int BATCH_ONCE_SEGMENT_COUNT_PWRITE = 4;

    private ByteBuffer readPageBuffer;
    private long readPageAddress;

    // from wal
    private ByteBuffer writePageBuffer;
    private long writePageAddress;
    private ByteBuffer writePageBufferB;
    private long writePageAddressB;

    // for repl
    ByteBuffer readForReplBuffer;
    private long readForReplAddress;

    ByteBuffer writeForReplBuffer;
    private long writeForReplAddress;

    // for chunk merge
    ByteBuffer readForMergeBatchBuffer;
    private long readForMergeBatchAddress;

    ByteBuffer readForTopMergeBatchBuffer;
    private long readForTopMergeBatchAddress;

    private int segmentLength;

    static final int MERGE_READ_SEGMENT_ONCE_COUNT = 10;

    private byte[][] allBytesBySegmentIndex;

    private LRUMap<Integer, byte[]> segmentBytesByIndex;

    public void initByteBuffers(boolean isChunkFd) {
        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.segmentLength = segmentLength;

        if (ConfForSlot.global.pureMemory) {
            var maxSegmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
            this.allBytesBySegmentIndex = new byte[maxSegmentNumberPerFd][];
        } else {
            var pageManager = PageManager.getInstance();
            var m = MemoryIO.getInstance();

            var oneSegmentPage = segmentLength / PAGE_SIZE;

            this.readPageAddress = pageManager.allocatePages(oneSegmentPage, PROTECTION);
            this.readPageBuffer = m.newDirectByteBuffer(readPageAddress, segmentLength);

            this.writePageAddress = pageManager.allocatePages(oneSegmentPage, PROTECTION);
            this.writePageBuffer = m.newDirectByteBuffer(writePageAddress, segmentLength);

            this.writePageAddressB = pageManager.allocatePages(oneSegmentPage * BATCH_ONCE_SEGMENT_COUNT_PWRITE, PROTECTION);
            this.writePageBufferB = m.newDirectByteBuffer(writePageAddressB, segmentLength * BATCH_ONCE_SEGMENT_COUNT_PWRITE);

            var replPages = oneSegmentPage * ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT;
            this.readForReplAddress = pageManager.allocatePages(replPages, PROTECTION);
            this.readForReplBuffer = m.newDirectByteBuffer(readForReplAddress, replPages * PAGE_SIZE);

            this.writeForReplAddress = pageManager.allocatePages(replPages, PROTECTION);
            this.writeForReplBuffer = m.newDirectByteBuffer(writeForReplAddress, replPages * PAGE_SIZE);

            if (isChunkFd) {
                // only merge worker need read for merging batch
                int npagesMerge = oneSegmentPage * MERGE_READ_SEGMENT_ONCE_COUNT;
                this.readForMergeBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
                this.readForMergeBatchBuffer = m.newDirectByteBuffer(readForMergeBatchAddress, npagesMerge * PAGE_SIZE);

                // only top merge worker need read self for merging batch
                this.readForTopMergeBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
                this.readForTopMergeBatchBuffer = m.newDirectByteBuffer(readForTopMergeBatchAddress, npagesMerge * PAGE_SIZE);
            }
        }
    }

    public void cleanUp() {
        stopEventLoop();

        var oneSegmentPage = segmentLength / PAGE_SIZE;

        var pageManager = PageManager.getInstance();
        if (readPageAddress != 0) {
            pageManager.freePages(readPageAddress, oneSegmentPage);
            System.out.println("Clean up fd read, name: " + name + ", read page address: " + readPageAddress);

            readPageAddress = 0;
            readPageBuffer = null;
        }

        if (writePageAddress != 0) {
            pageManager.freePages(writePageAddress, oneSegmentPage);
            System.out.println("Clean up fd write, name: " + name + ", write page address: " + writePageAddress);

            writePageAddress = 0;
            writePageBuffer = null;
        }

        if (writePageAddressB != 0) {
            pageManager.freePages(writePageAddressB, oneSegmentPage * BATCH_ONCE_SEGMENT_COUNT_PWRITE);
            System.out.println("Clean up fd write batch, name: " + name + ", write page address: " + writePageAddress);

            writePageAddressB = 0;
            writePageBufferB = null;
        }

        var replPages = oneSegmentPage * ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT;
        if (readForReplAddress != 0) {
            pageManager.freePages(readForReplAddress, replPages);
            System.out.println("Clean up fd read repl, name: " + name + ", read page address: " + readForReplAddress);

            readForReplAddress = 0;
            readForReplBuffer = null;
        }

        if (writeForReplAddress != 0) {
            pageManager.freePages(writeForReplAddress, replPages);
            System.out.println("Clean up fd write repl, name: " + name + ", write page address: " + writeForReplAddress);

            writeForReplAddress = 0;
            writeForReplBuffer = null;
        }

        int mergePages = oneSegmentPage * MERGE_READ_SEGMENT_ONCE_COUNT;
        if (readForMergeBatchAddress != 0) {
            pageManager.freePages(readForMergeBatchAddress, mergePages);
            System.out.println("Clean up fd read merge batch, name: " + name + ", read page address: " + readForMergeBatchAddress);

            readForMergeBatchAddress = 0;
            readForMergeBatchBuffer = null;
        }

        if (readForTopMergeBatchAddress != 0) {
            pageManager.freePages(readForTopMergeBatchAddress, mergePages);
            System.out.println("Clean up fd read self merge batch, name: " + name + ", read page address: " + readForTopMergeBatchAddress);

            readForTopMergeBatchAddress = 0;
            readForTopMergeBatchBuffer = null;
        }

        if (fd != 0) {
            int r = libC.close(fd);
            if (r < 0) {
                System.err.println("Close fd error: " + libC.strerror(r));
            }
            System.out.println("Closed fd: " + fd);
        }
    }

    @Override
    public List<StatKV> stats() {

        List<StatKV> list = new ArrayList<>();
//        final String prefix = name + " ";

        // todo
        var metricFamilySamples = readTimeSummary.collect();
        for (var metricFamilySample : metricFamilySamples) {
            for (var sample : metricFamilySample.samples) {
                list.add(new StatKV(sample.name, sample.value));
            }
        }
        list.add(StatKV.split);
        return list;
    }

    interface ReadBufferCallback {
        byte[] readFrom(ByteBuffer buffer);
    }

    interface WriteBufferPrepare {
        void prepare(ByteBuffer buffer);
    }

    private byte[] readInnerNotPureMemory(long offset, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        if (offset % segmentLength != 0) {
            throw new IllegalArgumentException("Offset must be multiple of segment length");
        }

        // for from lru cache if only read one segment
        int capacity = buffer.capacity();
        var isOnlyOneSegment = capacity == segmentLength;
        var segmentIndex = (int) (offset / segmentLength);
        if (isOnlyOneSegment) {
            var bytesCached = segmentBytesByIndex.get(segmentIndex);
            if (bytesCached != null) {
                return bytesCached;
            }
        }

        buffer.clear();

        var timer = readTimeSummary.labels(name).startTimer();
        var n = libC.pread(fd, buffer, capacity, offset);
        timer.observeDuration();
        readBytesCounter.labels(name).inc(n);
        if (n != capacity) {
            log.error("Read error, n: {}, buffer capacity: {}, name: {}", n, capacity, name);
            throw new RuntimeException("Read error, n: " + n + ", buffer capacity: " + capacity + ", name: " + name);
        }

        buffer.rewind();

        byte[] bytesRead;
        if (callback == null) {
            bytesRead = new byte[capacity];
            buffer.get(bytesRead);
        } else {
            bytesRead = callback.readFrom(buffer);
        }

        if (isOnlyOneSegment && isRefreshLRUCache) {
            segmentBytesByIndex.put(segmentIndex, bytesRead);
        }
        return bytesRead;
    }

    private int writeInnerNotPureMemory(long offset, ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        if (offset % segmentLength != 0) {
            throw new IllegalArgumentException("Offset must be multiple of segment length");
        }

        int capacity = buffer.capacity();
        var segmentCount = capacity / segmentLength;
        var isOnlyOneSegment = segmentCount == 1;
        var isPwriteBatch = segmentCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE;

        buffer.rewind();

        prepare.prepare(buffer);

        var timer = writeTimeSummary.labels(name).startTimer();
        var n = libC.pwrite(fd, buffer, capacity, offset);
        writeBytesCounter.labels(name).inc(n);
        timer.observeDuration();
        if (n != capacity) {
            log.error("Write error, n: {}, buffer capacity: {}, name: {}", n, capacity, name);
            throw new RuntimeException("Write error, n: " + n + ", buffer capacity: " + capacity + ", name: " + name);
        }

        // set to lru cache
        // lru bytes may be not full segment length when compress, todo
        if (isRefreshLRUCache && (isOnlyOneSegment || isPwriteBatch)) {
            var segmentIndex = (int) (offset / segmentLength);
            for (int i = 0; i < segmentCount; i++) {
                var bytes = new byte[segmentLength];
                buffer.position(i * segmentLength);
                buffer.get(bytes);
                segmentBytesByIndex.put(segmentIndex + i, bytes);
            }
        }

        return (int) n;
    }

    private CompletableFuture<byte[]> readAsyncNotPureMemory(long offset, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        return call(() -> readInnerNotPureMemory(offset, buffer, callback, isRefreshLRUCache));
    }

    private CompletableFuture<Integer> writeAsyncNotPureMemory(long offset, ByteBuffer buffer, WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        return call(() -> writeInnerNotPureMemory(offset, buffer, prepare, isRefreshLRUCache));
    }

    private CompletableFuture<byte[]> readSegmentBatchFromMemory(long offset, int segmentCount) {
        if (offset % segmentLength != 0) {
            throw new IllegalArgumentException("Offset must be multiple of segment length");
        }

        return call(() -> {
            var segmentIndex = (int) (offset / segmentLength);
            var bytesRead = new byte[segmentLength * segmentCount];
            for (int i = 0; i < segmentCount; i++) {
                var oneSegmentBytes = allBytesBySegmentIndex[segmentIndex + i];
                if (oneSegmentBytes != null) {
                    System.arraycopy(oneSegmentBytes, 0, bytesRead, i * segmentLength, segmentLength);
                }
            }
            return bytesRead;
        });
    }

    public CompletableFuture<byte[]> readSegment(long offset, boolean isRefreshLRUCache) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(offset, 1);
        }

        return readAsyncNotPureMemory(offset, readPageBuffer, null, isRefreshLRUCache);
    }

    public CompletableFuture<byte[]> readSegmentForRepl(long offset) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(offset, ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT);
        }

        return readAsyncNotPureMemory(offset, readForReplBuffer, null, false);
    }

    public CompletableFuture<byte[]> readSegmentForMerge(long offset) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(offset, BATCH_ONCE_SEGMENT_COUNT_PWRITE);
        }

        return readAsyncNotPureMemory(offset, readForMergeBatchBuffer, null, false);
    }

    public CompletableFuture<byte[]> readSegmentForTopMerge(long offset) {
        return readAsyncNotPureMemory(offset, readForTopMergeBatchBuffer, null, false);
    }

    private CompletableFuture<Integer> writeSegmentBatchToMemory(long offset, byte[] bytes) {
        if (bytes.length % segmentLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of segment length");
        }

        return call(() -> {
            var segmentIndex = (int) (offset / segmentLength);
            var segmentCount = bytes.length / segmentLength;
            for (int i = 0; i < segmentCount; i++) {
                allBytesBySegmentIndex[segmentIndex + i] = bytes;
            }
            return bytes.length;
        });
    }

    public CompletableFuture<Integer> writeSegment(long offset, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length != segmentLength) {
            throw new IllegalArgumentException("Write bytes length not match segment length");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(offset, bytes);
        }

        return writeAsyncNotPureMemory(offset, writePageBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public CompletableFuture<Integer> writeSegmentBatch(long offset, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(offset, bytes);
        }

        return writeAsyncNotPureMemory(offset, writePageBufferB, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public CompletableFuture<Integer> writeSegmentForRepl(long offset, byte[] bytes) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT) {
            throw new IllegalArgumentException("Repl write bytes length not match once repl segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(offset, bytes);
        }

        return writeAsyncNotPureMemory(offset, writeForReplBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, false);
    }
}
