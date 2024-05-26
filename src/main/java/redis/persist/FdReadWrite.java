package redis.persist;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import redis.ConfForSlot;
import redis.repl.content.ToMasterExistsSegmentMeta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;

import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

public class FdReadWrite extends ThreadSafeCaller {

    public FdReadWrite(String name, LibC libC, File file) throws IOException {
        this.name = name;
        this.libC = libC;

        if (!file.exists()) {
            FileUtils.touch(file);
        }
        this.fd = libC.open(file.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);
        log.info("Opened fd: {}, name: {}", fd, name);

        this.fdLength = (int) file.length();
    }

    private final String name;

    private final LibC libC;

    private final int fd;

    private int fdLength;

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

    @Override
    String threadName() {
        return "fd-read-write-" + name;
    }

    @Override
    ThreadFactory getNextThreadFactory() {
        return ThreadFactoryAssignSupport.getInstance().ForFdReadWrite.getNextThreadFactory();
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

    // for key bucket batch read
    ByteBuffer readForKeyBucketBatchBuffer;
    private long readForKeyBucketBatchAddress;

    private int segmentLength;

    static final int MERGE_READ_ONCE_SEGMENT_COUNT = 10;

    private byte[][] allBytesBySegmentIndex;

    public boolean isTargetSegmentIndexNullInMemory(int segmentIndex) {
        return allBytesBySegmentIndex[segmentIndex] == null;
    }

    private LRUMap<Integer, byte[]> segmentBytesByIndexLRU;

    public void initByteBuffers(boolean isChunkFd) {
        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.segmentLength = segmentLength;

        if (isChunkFd) {
            var allWorkers = ConfForSlot.global.allWorkers;
            var batchNumber = ConfForSlot.global.confWal.batchNumber;
            var maxSize = ConfForSlot.global.confChunk.lru.maxSize;
            var fdPerChunk = ConfForSlot.global.confChunk.fdPerChunk;

            // request worker's chunks lru size should be bigger than merge worker's chunks lru size, need dyn recreate lru map, todo
            int maxSizeOneFd = maxSize / allWorkers / batchNumber / fdPerChunk;
            log.info("Chunk lru max size for one worker/batch/fd: {}, segment length: {}, memory require: {}MB, total memory require: {}MB",
                    maxSizeOneFd, segmentLength, maxSizeOneFd * segmentLength / 1024 / 1024, maxSize * segmentLength / 1024 / 1024);
            this.segmentBytesByIndexLRU = new LRUMap<>(maxSizeOneFd);
        } else {
            // key bucket
            var slotNumber = ConfForSlot.global.slotNumber;
            var maxSize = ConfForSlot.global.confBucket.lru.maxSize;

            int maxSizeOneFd = maxSize / slotNumber;
            log.info("Bucket lru max size for one slot: {}, segment length: {}， memory require: {}MB, total memory require: {}MB",
                    maxSizeOneFd, segmentLength, maxSizeOneFd * segmentLength / 1024 / 1024, maxSize * segmentLength / 1024 / 1024);
            this.segmentBytesByIndexLRU = new LRUMap<>(maxSizeOneFd);
        }

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
            } else {
                int npagesBucket = ConfForSlot.global.confWal.oneChargeBucketNumber;
                this.readForKeyBucketBatchAddress = pageManager.allocatePages(npagesBucket, PROTECTION);
                this.readForKeyBucketBatchBuffer = m.newDirectByteBuffer(readForKeyBucketBatchAddress, npagesBucket * PAGE_SIZE);
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

        int bucketPages = oneSegmentPage * ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (readForKeyBucketBatchAddress != 0) {
            pageManager.freePages(readForKeyBucketBatchAddress, bucketPages);
            System.out.println("Clean up fd read key bucket batch, name: " + name + ", read page address: " + readForKeyBucketBatchAddress);

            readForKeyBucketBatchAddress = 0;
            readForKeyBucketBatchBuffer = null;
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
            var bytesCached = segmentBytesByIndexLRU.get(segmentIndex);
            if (bytesCached != null) {
                lruHitCounter.labels(name).inc();
                return bytesCached;
            } else {
                lruMissCounter.labels(name).inc();
            }
        }

        var offset = segmentIndex * segmentLength;
        if (offset >= fdLength) {
            return null;
        }

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
            segmentBytesByIndexLRU.put(segmentIndex, bytesRead);
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
        if (buffer.limit() != capacity) {
            // append 0
            var bytes0 = new byte[capacity - buffer.limit()];
            buffer.put(bytes0);
        }
        buffer.rewind();

        var timer = writeTimeSummary.labels(name).startTimer();
        var n = libC.pwrite(fd, buffer, capacity, offset);
        writeBytesCounter.labels(name).inc(n);
        timer.observeDuration();
        if (n != capacity) {
            log.error("Write error, n: {}, buffer capacity: {}, name: {}", n, capacity, name);
            throw new RuntimeException("Write error, n: " + n + ", buffer capacity: " + capacity + ", name: " + name);
        }

        if (offset + capacity > fdLength) {
            fdLength = offset + capacity;
        }

        // set to lru cache
        // lru bytes may be not full segment length when compress, todo
        if (isRefreshLRUCache && (isOnlyOneSegment || isPwriteBatch)) {
            for (int i = 0; i < segmentCount; i++) {
                var bytes = new byte[segmentLength];
                buffer.position(i * segmentLength);
                buffer.get(bytes);
                segmentBytesByIndexLRU.put(segmentIndex + i, bytes);
            }
        } else {
            for (int i = 0; i < segmentCount; i++) {
                segmentBytesByIndexLRU.remove(segmentIndex + i);
            }
        }

        return n;
    }

    private byte[] readAsyncNotPureMemory(int segmentIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        return callSync(() -> readInnerNotPureMemory(segmentIndex, buffer, callback, isRefreshLRUCache));
    }

    private int writeAsyncNotPureMemory(int segmentIndex, ByteBuffer buffer, WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        return callSync(() -> writeInnerNotPureMemory(segmentIndex, buffer, prepare, isRefreshLRUCache));
    }

    private byte[] readSegmentBatchFromMemory(int segmentIndex, int segmentCount) {
        return callSync(() -> {
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

    public byte[] readSegment(int segmentIndex, boolean isRefreshLRUCache) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, 1);
        }

        return readAsyncNotPureMemory(segmentIndex, readPageBuffer, null, isRefreshLRUCache);
    }

    public byte[] readSegmentForMerge(int segmentIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, MERGE_READ_ONCE_SEGMENT_COUNT);
        }

        return readAsyncNotPureMemory(segmentIndex, readForMergeBatchBuffer, null, false);
    }

    public byte[] readSegmentForRepl(int segmentIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT);
        }

        return readAsyncNotPureMemory(segmentIndex, readForReplBuffer, null, false);
    }

    public byte[] readSegmentForKeyBucketWhenMergeCompare(int bucketIndex) {
        // bucketIndex is segmentIndex when fd is key bucket fd, not chunk fd
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(bucketIndex, ConfForSlot.global.confWal.oneChargeBucketNumber);
        }

        return readAsyncNotPureMemory(bucketIndex, readForKeyBucketBatchBuffer, null, false);
    }

    public int clearOneSegmentToMemory(int segmentIndex) {
        return callSync(() -> {
            allBytesBySegmentIndex[segmentIndex] = null;
            return 0;
        });
    }

    public int writeSegmentBatchToMemory(int segmentIndex, byte[] bytes, int position) {
        var isSmallerThanOneSegment = bytes.length < segmentLength;
        if (!isSmallerThanOneSegment && bytes.length % segmentLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of segment length");
        }

        return callSync(() -> {
            if (isSmallerThanOneSegment) {
                allBytesBySegmentIndex[segmentIndex] = bytes;
                return bytes.length;
            }

            var segmentCount = bytes.length / segmentLength;
            var offset = position;
            for (int i = 0; i < segmentCount; i++) {
                var bytesOneSegment = new byte[segmentLength];
                System.arraycopy(bytes, offset, bytesOneSegment, 0, segmentLength);
                offset += segmentLength;

                allBytesBySegmentIndex[segmentIndex + i] = bytesOneSegment;
            }
            return bytes.length;
        });
    }

    public int writeSegment(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > segmentLength) {
            throw new IllegalArgumentException("Write bytes length must be less than segment length");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes, 0);
        }

        return writeAsyncNotPureMemory(segmentIndex, writePageBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeSegmentBatch(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        return writeSegmentBatch(segmentIndex, bytes, 0, isRefreshLRUCache);
    }

    public int writeSegmentBatch(int segmentIndex, byte[] bytes, int position, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes, position);
        }

        return writeAsyncNotPureMemory(segmentIndex, writePageBufferB, (buffer) -> {
            buffer.clear();
            if (position == 0) {
                buffer.put(bytes);
            } else {
                buffer.put(bytes, position, bytes.length - position);
            }
        }, isRefreshLRUCache);
    }

    public int writeSegmentForRepl(int segmentIndex, byte[] bytes, int position) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT) {
            throw new IllegalArgumentException("Repl write bytes length not match once repl segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes, position);
        }

        return writeAsyncNotPureMemory(segmentIndex, writeForReplBuffer, (buffer) -> {
            buffer.clear();
            if (position == 0) {
                buffer.put(bytes);
            } else {
                buffer.put(bytes, position, bytes.length - position);
            }
        }, false);
    }

    public int truncate() {
        return callSync(() -> {
            var r = libC.ftruncate(fd, 0);
            if (r < 0) {
                throw new RuntimeException("Truncate error: " + libC.strerror(r));
            }
            log.info("Truncate fd: {}, name: {}", fd, name);
            return r;
        });
    }
}
