package redis.persist;

import com.github.luben.zstd.Zstd;
import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.metric.SimpleGauge;
import redis.repl.content.ToMasterExistsSegmentMeta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

// thread safe
public class FdReadWrite {

    private final Logger log = LoggerFactory.getLogger(FdReadWrite.class);

    // for unit test
    public FdReadWrite(String name) {
        this.name = name;
        this.libC = null;
        this.fd = 0;
    }

    public FdReadWrite(String name, LibC libC, File file) throws IOException {
        this.name = name;
        this.libC = libC;

        if (!file.exists()) {
            FileUtils.touch(file);
        }
        this.fd = libC.open(file.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);
        this.writeIndex = file.length();
        log.info("Opened fd: {}, name: {}, file length: {}MB", fd, name, this.writeIndex / 1024 / 1024);

        fdReadWriteGauge.addRawGetter(() -> {
            var labelValues = List.of(name);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("fd_length", new SimpleGauge.ValueWithLabelValues((double) writeIndex, labelValues));

            map.put("after_lru_read_decompress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) afterLRUReadDecompressTimeTotalUs, labelValues));
            if (lruHitCounter > 0) {
                map.put("after_lru_read_decompress_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) afterLRUReadDecompressTimeTotalUs / lruHitCounter, labelValues));
            }

            map.put("after_fd_pread_compress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) afterFdPreadCompressTimeTotalUs, labelValues));
            map.put("after_fd_pread_compress_count_total", new SimpleGauge.ValueWithLabelValues((double) afterFdPreadCompressCountTotal, labelValues));
            if (afterFdPreadCompressCountTotal > 0) {
                map.put("after_fd_pread_compress_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) afterFdPreadCompressTimeTotalUs / afterFdPreadCompressCountTotal, labelValues));
            }

            map.put("read_bytes_total", new SimpleGauge.ValueWithLabelValues((double) readBytesTotal, labelValues));
            map.put("read_time_total_us", new SimpleGauge.ValueWithLabelValues((double) readTimeTotalUs, labelValues));
            map.put("read_count_total", new SimpleGauge.ValueWithLabelValues((double) readCountTotal, labelValues));
            if (readCountTotal > 0) {
                map.put("read_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) readTimeTotalUs / readCountTotal, labelValues));
            }

            map.put("write_bytes_total", new SimpleGauge.ValueWithLabelValues((double) writeBytesTotal, labelValues));
            map.put("write_time_total_us", new SimpleGauge.ValueWithLabelValues((double) writeTimeTotalUs, labelValues));
            map.put("write_count_total", new SimpleGauge.ValueWithLabelValues((double) writeCountTotal, labelValues));
            if (writeCountTotal > 0) {
                map.put("write_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) writeTimeTotalUs / writeCountTotal, labelValues));
            }

            map.put("lru_hit_counter", new SimpleGauge.ValueWithLabelValues((double) lruHitCounter, labelValues));
            map.put("lru_miss_counter", new SimpleGauge.ValueWithLabelValues((double) lruMissCounter, labelValues));

            return map;
        });
    }

    private final String name;

    private final LibC libC;

    private final int fd;

    long writeIndex;

    private boolean isLRUOn = false;

    private final static SimpleGauge fdReadWriteGauge = new SimpleGauge("fd_read_write", "chunk or key buckets file read write",
            "name");

    static {
        fdReadWriteGauge.register();
    }

    // avg = this time / cache hit count
    private long afterLRUReadDecompressTimeTotalUs;

    // avg = this time / after fd pread compress count total
    private long afterFdPreadCompressTimeTotalUs;
    private long afterFdPreadCompressCountTotal;

    private long readBytesTotal;
    private long readTimeTotalUs;
    private long readCountTotal;

    private long writeBytesTotal;
    private long writeTimeTotalUs;
    private long writeCountTotal;

    private long lruHitCounter;
    private long lruMissCounter;

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

    // for key bucket batch read / write
    ByteBuffer readForOneWalGroupBatchBuffer;
    private long readForOneWalGroupBatchAddress;
    ByteBuffer writeForOneWalGroupBatchBuffer;
    private long writeForOneWalGroupBatchAddress;

    private int segmentLength;
    boolean isChunkFd;

    static final int MERGE_READ_ONCE_SEGMENT_COUNT = 10;

    private byte[][] allBytesBySegmentIndex;

    public boolean isTargetSegmentIndexNullInMemory(int segmentIndex) {
        return allBytesBySegmentIndex[segmentIndex] == null;
    }

    private LRUMap<Integer, byte[]> segmentBytesByIndexLRU;

    public void initByteBuffers(boolean isChunkFd) {
        var segmentLength = isChunkFd ? ConfForSlot.global.confChunk.segmentLength : KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
        this.segmentLength = segmentLength;
        this.isChunkFd = isChunkFd;

        if (isChunkFd) {
            var maxSize = ConfForSlot.global.confChunk.lruPerFd.maxSize;
            var lruMemoryRequireMB = 1L * maxSize * segmentLength / 1024 / 1024;
            log.info("Chunk lru max size for one chunk fd: {}, segment length: {}, memory require: {}MB, name: {}",
                    maxSize, lruMemoryRequireMB, name);
            LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_chunk_data, (int) lruMemoryRequireMB, true);

            if (maxSize > 0) {
                this.segmentBytesByIndexLRU = new LRUMap<>(maxSize);
                this.isLRUOn = true;
            }
        } else {
            // key bucket
            var maxSize = ConfForSlot.global.confBucket.lruPerFd.maxSize;
            // need to compare with metrics
            final var compressRatio = 0.25;
            var lruMemoryRequireMB = 1L * maxSize * segmentLength / 1024 / 1024 * compressRatio;
            log.info("Key bucket lru max size for one key bucket fd: {}, segment length: {}， compress ratio maybe: {}, memory require: {}MB, name: {}",
                    maxSize, segmentLength, compressRatio, lruMemoryRequireMB, name);
            LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_key_bucket, (int) lruMemoryRequireMB, false);

            if (maxSize > 0) {
                this.segmentBytesByIndexLRU = new LRUMap<>(maxSize);
                this.isLRUOn = true;
            }
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

            var npagesRepl = oneSegmentPage * ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT;
            this.readForReplAddress = pageManager.allocatePages(npagesRepl, PROTECTION);
            this.readForReplBuffer = m.newDirectByteBuffer(readForReplAddress, npagesRepl * PAGE_SIZE);

            this.writeForReplAddress = pageManager.allocatePages(npagesRepl, PROTECTION);
            this.writeForReplBuffer = m.newDirectByteBuffer(writeForReplAddress, npagesRepl * PAGE_SIZE);

            if (isChunkFd) {
                // only merge worker need read for merging batch
                int npagesMerge = oneSegmentPage * MERGE_READ_ONCE_SEGMENT_COUNT;
                this.readForMergeBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
                this.readForMergeBatchBuffer = m.newDirectByteBuffer(readForMergeBatchAddress, npagesMerge * PAGE_SIZE);
            } else {
                int npagesBucket = ConfForSlot.global.confWal.oneChargeBucketNumber;
                this.readForOneWalGroupBatchAddress = pageManager.allocatePages(npagesBucket, PROTECTION);
                this.readForOneWalGroupBatchBuffer = m.newDirectByteBuffer(readForOneWalGroupBatchAddress, npagesBucket * PAGE_SIZE);

                this.writeForOneWalGroupBatchAddress = pageManager.allocatePages(npagesBucket, PROTECTION);
                this.writeForOneWalGroupBatchBuffer = m.newDirectByteBuffer(writeForOneWalGroupBatchAddress, npagesBucket * PAGE_SIZE);
            }
        }
    }

    public void cleanUp() {
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

        var npagesRepl = oneSegmentPage * ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT;
        if (readForReplAddress != 0) {
            pageManager.freePages(readForReplAddress, npagesRepl);
            System.out.println("Clean up fd read repl, name: " + name + ", read page address: " + readForReplAddress);

            readForReplAddress = 0;
            readForReplBuffer = null;
        }

        if (writeForReplAddress != 0) {
            pageManager.freePages(writeForReplAddress, npagesRepl);
            System.out.println("Clean up fd write repl, name: " + name + ", write page address: " + writeForReplAddress);

            writeForReplAddress = 0;
            writeForReplBuffer = null;
        }

        int npagesMerge = oneSegmentPage * MERGE_READ_ONCE_SEGMENT_COUNT;
        if (readForMergeBatchAddress != 0) {
            pageManager.freePages(readForMergeBatchAddress, npagesMerge);
            System.out.println("Clean up fd read merge batch, name: " + name + ", read page address: " + readForMergeBatchAddress);

            readForMergeBatchAddress = 0;
            readForMergeBatchBuffer = null;
        }

        int npagesBucket = ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (readForOneWalGroupBatchAddress != 0) {
            pageManager.freePages(readForOneWalGroupBatchAddress, npagesBucket);
            System.out.println("Clean up fd read for one wal group batch, name: " + name + ", read page address: " + readForOneWalGroupBatchAddress);

            readForOneWalGroupBatchAddress = 0;
            readForOneWalGroupBatchBuffer = null;
        }

        if (writeForOneWalGroupBatchAddress != 0) {
            pageManager.freePages(writeForOneWalGroupBatchAddress, npagesBucket);
            System.out.println("Clean up fd write for one wal group batch, name: " + name + ", write page address: " + writeForOneWalGroupBatchAddress);

            writeForOneWalGroupBatchAddress = 0;
            writeForOneWalGroupBatchBuffer = null;
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
        byte[] readFrom(ByteBuffer buffer, int readLength);
    }

    interface WriteBufferPrepare {
        void prepare(ByteBuffer buffer);
    }

    private void checkSegmentIndex(int segmentIndex) {
        if (isChunkFd) {
            assert segmentIndex >= 0 && segmentIndex < ConfForSlot.global.confChunk.segmentNumberPerFd;
        } else {
            // segment index -> bucket index
            assert segmentIndex >= 0 && segmentIndex < ConfForSlot.global.confBucket.bucketsPerSlot;
        }
    }

    private byte[] readInnerNotPureMemory(int segmentIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        return readInnerNotPureMemory(segmentIndex, buffer, callback, isRefreshLRUCache, buffer.capacity());
    }

    private byte[] readInnerNotPureMemory(int segmentIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache, int length) {
        checkSegmentIndex(segmentIndex);
        if (length > buffer.capacity()) {
            throw new IllegalArgumentException("Read length must be less than buffer capacity: " + buffer.capacity() + ", read length: " + length);
        }

        // for from lru cache if only read one segment
        var isOnlyOneSegment = length == segmentLength;
        int segmentCount;
        if (isOnlyOneSegment) {
            segmentCount = 1;

            if (isLRUOn) {
                var bytesCached = segmentBytesByIndexLRU.get(segmentIndex);
                if (bytesCached != null) {
                    lruHitCounter++;

                    if (isChunkFd) {
                        return bytesCached;
                    } else {
                        // only key bucket data need decompress
                        var beginT = System.nanoTime();
                        var bytesDecompressedCached = Zstd.decompress(bytesCached, segmentLength);
                        var costT = (System.nanoTime() - beginT) / 1000;
                        if (costT == 0) {
                            costT = 1;
                        }
                        afterLRUReadDecompressTimeTotalUs += costT;
                        return bytesDecompressedCached;
                    }
                } else {
                    lruMissCounter++;
                }
            }
        } else {
            segmentCount = length / segmentLength;
        }

        var offset = segmentIndex * segmentLength;
        if (offset >= writeIndex) {
            return null;
        }

        int readLength = length;
        var lastSegmentOffset = offset + (segmentCount * segmentLength);
        if (writeIndex <= lastSegmentOffset) {
            readLength = (int) (writeIndex - offset);
        }
        if (readLength < 0 || readLength > length) {
            throw new IllegalArgumentException("Read length must be less than given length: " + length + ", read length: " + readLength);
        }

        buffer.clear();

        readCountTotal++;
        var beginT = System.nanoTime();
        var n = libC.pread(fd, buffer, readLength, offset);
        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        readTimeTotalUs += costT;
        readBytesTotal += n;

        if (n != readLength) {
            log.error("Read error, n: {}, read length: {}, name: {}", n, readLength, name);
            throw new RuntimeException("Read error, n: " + n + ", read length: " + readLength + ", name: " + name);
        }

        buffer.rewind();

        byte[] bytesRead;
        if (callback == null) {
            bytesRead = new byte[readLength];
            buffer.get(bytesRead);
        } else {
            bytesRead = callback.readFrom(buffer, readLength);
        }

        if (isOnlyOneSegment && isRefreshLRUCache) {
            if (isLRUOn) {
                if (isChunkFd) {
                    segmentBytesByIndexLRU.put(segmentIndex, bytesRead);
                } else {
                    // only key bucket data need compress
                    var beginT2 = System.nanoTime();
                    var bytesCompressed = Zstd.compress(bytesRead);
                    var costT2 = (System.nanoTime() - beginT2) / 1000;
                    if (costT2 == 0) {
                        costT2 = 1;
                    }
                    afterFdPreadCompressTimeTotalUs += costT2;
                    afterFdPreadCompressCountTotal++;
                    segmentBytesByIndexLRU.put(segmentIndex, bytesCompressed);
                }
            }
        }
        return bytesRead;
    }

    private int writeInnerNotPureMemory(int segmentIndex, ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        checkSegmentIndex(segmentIndex);

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

        writeCountTotal++;
        var beginT = System.nanoTime();
        var n = libC.pwrite(fd, buffer, capacity, offset);
        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        writeTimeTotalUs += costT;
        writeBytesTotal += n;

        if (n != capacity) {
            log.error("Write error, n: {}, buffer capacity: {}, name: {}", n, capacity, name);
            throw new RuntimeException("Write error, n: " + n + ", buffer capacity: " + capacity + ", name: " + name);
        }

        if (offset + capacity > writeIndex) {
            writeIndex = offset + capacity;
        }

        // set to lru cache
        if (isLRUOn) {
            if (isRefreshLRUCache && (isOnlyOneSegment || isPwriteBatch)) {
                for (int i = 0; i < segmentCount; i++) {
                    var bytes = new byte[segmentLength];
                    buffer.position(i * segmentLength);
                    buffer.get(bytes);

                    // chunk data is already compressed
                    // key bucket data need not compress here, compress when after read
                    if (isChunkFd) {
                        segmentBytesByIndexLRU.put(segmentIndex + i, bytes);
                    }
                }
            } else {
                for (int i = 0; i < segmentCount; i++) {
                    segmentBytesByIndexLRU.remove(segmentIndex + i);
                }
            }
        }

        return n;
    }

    private byte[] readSegmentBatchFromMemory(int segmentIndex, int segmentCount) {
        var bytesRead = new byte[segmentLength * segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            var oneSegmentBytes = allBytesBySegmentIndex[segmentIndex + i];
            if (oneSegmentBytes != null) {
                System.arraycopy(oneSegmentBytes, 0, bytesRead, i * segmentLength, oneSegmentBytes.length);
            }
        }
        return bytesRead;
    }

    public byte[] readSegment(int segmentIndex, boolean isRefreshLRUCache) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, 1);
        }

        return readInnerNotPureMemory(segmentIndex, readPageBuffer, null, isRefreshLRUCache);
    }

    public byte[] readSegmentForMerge(int segmentIndex, int segmentCount) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, segmentCount);
        }

        return readInnerNotPureMemory(segmentIndex, readForMergeBatchBuffer, null, false, segmentCount * segmentLength);
    }

    public byte[] readSegmentForRepl(int segmentIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(segmentIndex, ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT);
        }

        return readInnerNotPureMemory(segmentIndex, readForReplBuffer, null, false);
    }

    public byte[] readSegmentForKeyBucketsInOneWalGroup(int bucketIndex) {
        // bucketIndex is segmentIndex when fd is key bucket fd, not chunk fd
        if (ConfForSlot.global.pureMemory) {
            return readSegmentBatchFromMemory(bucketIndex, ConfForSlot.global.confWal.oneChargeBucketNumber);
        }

        return readInnerNotPureMemory(bucketIndex, readForOneWalGroupBatchBuffer, null, false);
    }

    public int clearOneSegmentToMemory(int segmentIndex) {
        allBytesBySegmentIndex[segmentIndex] = null;
        return 0;
    }

    public int writeSegmentBatchToMemory(int segmentIndex, byte[] bytes, int position) {
        var isSmallerThanOneSegment = bytes.length < segmentLength;
        if (!isSmallerThanOneSegment && bytes.length % segmentLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of segment length");
        }

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
    }

    public int writeSegment(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > segmentLength) {
            throw new IllegalArgumentException("Write bytes length must be less than segment length");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes, 0);
        }

        return writeInnerNotPureMemory(segmentIndex, writePageBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeSegmentBatch(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            final int position = 0;
            return writeSegmentBatchToMemory(segmentIndex, bytes, position);
        }

        return writeInnerNotPureMemory(segmentIndex, writePageBufferB, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeSegmentForKeyBucketsInOneWalGroup(int segmentIndex, byte[] bytes) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != ConfForSlot.global.confWal.oneChargeBucketNumber) {
            throw new IllegalArgumentException("Batch write bytes length not match one charge bucket number in one wal group");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes, 0);
        }

        return writeInnerNotPureMemory(segmentIndex, writeForOneWalGroupBatchBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, false);
    }

    public int writeSegmentForRepl(int segmentIndex, byte[] bytes, int position) {
        var segmentCount = bytes.length / segmentLength;
        if (segmentCount != ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT) {
            throw new IllegalArgumentException("Repl write bytes length not match once repl segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeSegmentBatchToMemory(segmentIndex, bytes, position);
        }

        return writeInnerNotPureMemory(segmentIndex, writeForReplBuffer, (buffer) -> {
            buffer.clear();
            if (position == 0) {
                buffer.put(bytes);
            } else {
                buffer.put(bytes, position, bytes.length - position);
            }
        }, false);
    }

    public int truncate() {
        var r = libC.ftruncate(fd, 0);
        if (r < 0) {
            throw new RuntimeException("Truncate error: " + libC.strerror(r));
        }
        log.info("Truncate fd: {}, name: {}", fd, name);
        return r;
    }
}
