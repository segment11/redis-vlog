package redis.persist;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import io.activej.async.callback.AsyncComputation;
import io.activej.eventloop.Eventloop;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.repl.content.ToMasterExistsSegmentMeta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

public class FdReadWrite {

    public FdReadWrite(String name, LibC libC, File file) throws IOException {
        this.name = name;
        this.libC = libC;

        if (!file.exists()) {
            FileUtils.touch(file);
        }
        this.fd = libC.open(file.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);
        log.info("Opened fd: {}, name: {}", fd, name);
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

    private static final Counter lruHitCounter = Counter.build().name("lru_hit").
            help("fd read lru hit").
            labelNames("fd_name")
            .register();

    private static final Counter lruMissCounter = Counter.build().name("lru_miss").
            help("fd read lru miss").
            labelNames("fd_name")
            .register();

    private Eventloop eventloop;

    private long threadId;

    private final int idleMillis = 10;

    private final Logger log = LoggerFactory.getLogger(FdReadWrite.class);

    // one or two ssd volume, one cpu v-core is enough
    static final ThreadFactory threadFactoryGroup1 = new AffinityThreadFactory("merge-worker-group1",
            AffinityStrategies.SAME_CORE);

    static final ThreadFactory threadFactoryGroup2 = new AffinityThreadFactory("merge-worker-group1",
            AffinityStrategies.SAME_CORE);

    static final ThreadFactory threadFactoryGroup3 = new AffinityThreadFactory("merge-worker-group1",
            AffinityStrategies.SAME_CORE);

    static final ThreadFactory threadFactoryGroup4 = new AffinityThreadFactory("merge-worker-group1",
            AffinityStrategies.SAME_CORE);

    private static final int THREAD_FACTORY_GROUP_COUNT = 4;

    // need not thread safe
    // just for init
    private static int increaseCountForChooseThreadFactory = 0;

    private static ThreadFactory getNextThreadFactory() {
        int d = increaseCountForChooseThreadFactory % THREAD_FACTORY_GROUP_COUNT;
        var threadFactory = switch (d) {
            case 0 -> threadFactoryGroup1;
            case 1 -> threadFactoryGroup2;
            case 2 -> threadFactoryGroup3;
            case 3 -> threadFactoryGroup4;
            default -> throw new IllegalStateException("Unexpected value: " + d);
        };
        increaseCountForChooseThreadFactory++;
        return threadFactory;
    }

    public void initEventloop(ThreadFactory threadFactoryGiven) {
        var threadName = "fd-read-write-" + name;
        this.eventloop = Eventloop.builder()
                .withThreadName(threadName)
                .withIdleInterval(Duration.ofMillis(idleMillis))
                .build();
        this.eventloop.keepAlive(true);

        var threadFactory = threadFactoryGiven == null ? getNextThreadFactory() : threadFactoryGiven;
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

    private int segmentLength;

    static final int MERGE_READ_ONCE_SEGMENT_COUNT = 10;

    private byte[][] allBytesBySegmentIndex;

    public boolean isTargetSegmentIndexNullInMemory(int segmentIndex) {
        return allBytesBySegmentIndex[segmentIndex] == null;
    }

    private LRUMap<Integer, byte[]> segmentBytesByIndex;

    public void initByteBuffers(boolean isChunkFd) {
        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.segmentLength = segmentLength;

        var allWorkers = ConfForSlot.global.allWorkers;
        var batchNumber = ConfForSlot.global.confWal.batchNumber;
        var maxSize = ConfForSlot.global.confChunk.lru.maxSize;
        var fdPerChunk = ConfForSlot.global.confChunk.fdPerChunk;

        // request worker's chunks lru size should be bigger than merge worker's chunks lru size, need dyn recreate lru map, todo
        int maxSizeOneFd = maxSize / allWorkers / batchNumber / fdPerChunk;
        log.info("Chunk lru max size for one fd: {}, segment length: {}， total memory require: {}MB",
                maxSizeOneFd, segmentLength, maxSize * segmentLength / 1024 / 1024);
        this.segmentBytesByIndex = new LRUMap<>(maxSizeOneFd);

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
                int npagesMerge = oneSegmentPage * MERGE_READ_ONCE_SEGMENT_COUNT;
                this.readForMergeBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
                this.readForMergeBatchBuffer = m.newDirectByteBuffer(readForMergeBatchAddress, npagesMerge * PAGE_SIZE);
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

        int mergePages = oneSegmentPage * MERGE_READ_ONCE_SEGMENT_COUNT;
        if (readForMergeBatchAddress != 0) {
            pageManager.freePages(readForMergeBatchAddress, mergePages);
            System.out.println("Clean up fd read merge batch, name: " + name + ", read page address: " + readForMergeBatchAddress);

            readForMergeBatchAddress = 0;
            readForMergeBatchBuffer = null;
        }

        if (fd != 0) {
            int r = libC.close(fd);
            if (r < 0) {
                System.err.println("Close fd error: " + libC.strerror(r));
            }
            System.out.println("Closed fd: " + fd);
        }
    }

    // do not use cpu calculate
    interface ReadBufferCallback {
        byte[] readFrom(ByteBuffer buffer);
    }

    interface WriteBufferPrepare {
        void prepare(ByteBuffer buffer);
    }

    private byte[] readInnerNotPureMemory(int segmentIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        // for from lru cache if only read one segment
        int capacity = buffer.capacity();
        var isOnlyOneSegment = capacity == segmentLength;
        if (isOnlyOneSegment) {
            var bytesCached = segmentBytesByIndex.get(segmentIndex);
            if (bytesCached != null) {
                lruHitCounter.labels(name).inc();
                return bytesCached;
            } else {
                lruMissCounter.labels(name).inc();
            }
        }

        var offset = segmentIndex * segmentLength;
        buffer.clear();

        var timer = readTimeSummary.labels(name).startTimer();
        var n = libC.pread(fd, buffer, capacity, offset);
        readBytesCounter.labels(name).inc(n);
        timer.observeDuration();

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

    private int writeInnerNotPureMemory(int segmentIndex, ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        int capacity = buffer.capacity();
        var segmentCount = capacity / segmentLength;
        var isOnlyOneSegment = segmentCount == 1;
        var isPwriteBatch = segmentCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE;

        int offset = segmentIndex * segmentLength;
        buffer.clear();

        prepare.prepare(buffer);
        buffer.rewind();

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
            for (int i = 0; i < segmentCount; i++) {
                var bytes = new byte[segmentLength];
                buffer.position(i * segmentLength);
                buffer.get(bytes);
                segmentBytesByIndex.put(segmentIndex + i, bytes);
            }
        }

        return (int) n;
    }

    private CompletableFuture<byte[]> readAsyncNotPureMemory(int segmentIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        return call(() -> readInnerNotPureMemory(segmentIndex, buffer, callback, isRefreshLRUCache));
    }

    private CompletableFuture<Integer> writeAsyncNotPureMemory(int segmentIndex, ByteBuffer buffer, WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        return call(() -> writeInnerNotPureMemory(segmentIndex, buffer, prepare, isRefreshLRUCache));
    }

    private CompletableFuture<byte[]> readSegmentBatchFromMemory(int segmentIndex, int segmentCount) {
        return call(() -> {
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

    public CompletableFuture<byte[]> readSegment(int segmentIndex, boolean isRefreshLRUCache) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, 1);
        }

        return readAsyncNotPureMemory(segmentIndex, readPageBuffer, null, isRefreshLRUCache);
    }

    public CompletableFuture<byte[]> readSegmentForMerge(int segmentIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, MERGE_READ_ONCE_SEGMENT_COUNT);
        }

        return readAsyncNotPureMemory(segmentIndex, readForMergeBatchBuffer, null, false);
    }

    public CompletableFuture<byte[]> readSegmentForRepl(int segmentIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT);
        }

        return readAsyncNotPureMemory(segmentIndex, readForReplBuffer, null, false);
    }

    private CompletableFuture<Integer> writeSegmentBatchToMemory(int segmentIndex, byte[] bytes) {
        if (bytes.length % segmentLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of segment length");
        }

        return call(() -> {
            var segmentCount = bytes.length / segmentLength;
            for (int i = 0; i < segmentCount; i++) {
                allBytesBySegmentIndex[segmentIndex + i] = bytes;
            }
            return bytes.length;
        });
    }

    public CompletableFuture<Integer> writeSegment(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > segmentLength) {
            throw new IllegalArgumentException("Write bytes length must be less than segment length");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes);
        }

        return writeAsyncNotPureMemory(segmentIndex, writePageBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public CompletableFuture<Integer> writeSegmentBatch(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes);
        }

        return writeAsyncNotPureMemory(segmentIndex, writePageBufferB, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public CompletableFuture<Integer> writeSegmentForRepl(int segmentIndex, byte[] bytes) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT) {
            throw new IllegalArgumentException("Repl write bytes length not match once repl segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes);
        }

        return writeAsyncNotPureMemory(segmentIndex, writeForReplBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, false);
    }
}
