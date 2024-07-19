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

        this.initMetricsCollect(name);
    }

    private void initMetricsCollect(String name) {
        fdReadWriteGauge.addRawGetter(() -> {
            var labelValues = List.of(name);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("fd_length", new SimpleGauge.ValueWithLabelValues((double) writeIndex, labelValues));

            if (afterFdPreadCompressCountTotal > 0) {
                map.put("after_fd_pread_compress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) afterFdPreadCompressTimeTotalUs, labelValues));
                map.put("after_fd_pread_compress_count_total", new SimpleGauge.ValueWithLabelValues((double) afterFdPreadCompressCountTotal, labelValues));
                map.put("after_fd_pread_compress_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) afterFdPreadCompressTimeTotalUs / afterFdPreadCompressCountTotal, labelValues));
            }

            if (readCountTotal > 0) {
                map.put("read_bytes_total", new SimpleGauge.ValueWithLabelValues((double) readBytesTotal, labelValues));
                map.put("read_time_total_us", new SimpleGauge.ValueWithLabelValues((double) readTimeTotalUs, labelValues));
                map.put("read_count_total", new SimpleGauge.ValueWithLabelValues((double) readCountTotal, labelValues));
                map.put("read_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) readTimeTotalUs / readCountTotal, labelValues));
            }

            if (writeCountTotal > 0) {
                map.put("write_bytes_total", new SimpleGauge.ValueWithLabelValues((double) writeBytesTotal, labelValues));
                map.put("write_time_total_us", new SimpleGauge.ValueWithLabelValues((double) writeTimeTotalUs, labelValues));
                map.put("write_count_total", new SimpleGauge.ValueWithLabelValues((double) writeCountTotal, labelValues));
                map.put("write_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) writeTimeTotalUs / writeCountTotal, labelValues));
            }

            if (lruHitCounter > 0) {
                map.put("lru_hit_counter", new SimpleGauge.ValueWithLabelValues((double) lruHitCounter, labelValues));

                map.put("after_lru_read_decompress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) afterLRUReadDecompressTimeTotalUs, labelValues));
                map.put("after_lru_read_decompress_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) afterLRUReadDecompressTimeTotalUs / lruHitCounter, labelValues));
            }
            if (lruMissCounter > 0) {
                map.put("lru_miss_counter", new SimpleGauge.ValueWithLabelValues((double) lruMissCounter, labelValues));
            }

            return map;
        });
    }

    final String name;

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

    private int oneInnerLength;
    boolean isChunkFd;

    static final int MERGE_READ_ONCE_SEGMENT_COUNT = 10;

    byte[][] allBytesByOneInnerIndex;

    public boolean isTargetSegmentIndexNullInMemory(int oneInnerIndex) {
        return allBytesByOneInnerIndex[oneInnerIndex] == null;
    }

    private LRUMap<Integer, byte[]> segmentBytesByIndexLRU;

    public void initByteBuffers(boolean isChunkFd) {
        var oneInnerLength = isChunkFd ? ConfForSlot.global.confChunk.segmentLength : KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
        this.oneInnerLength = oneInnerLength;
        this.isChunkFd = isChunkFd;

        if (isChunkFd) {
            var maxSize = ConfForSlot.global.confChunk.lruPerFd.maxSize;
            var lruMemoryRequireMB = 1L * maxSize * oneInnerLength / 1024 / 1024;
            log.info("Chunk lru max size for one chunk fd: {}, one inner length: {}, memory require: {}MB, name: {}",
                    maxSize, oneInnerLength, lruMemoryRequireMB, name);
            log.info("LRU prepare, type: {}, MB: {}, fd: {}", LRUPrepareBytesStats.Type.fd_chunk_data, lruMemoryRequireMB, name);
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
            var lruMemoryRequireMB = 1L * maxSize * oneInnerLength / 1024 / 1024 * compressRatio;
            log.info("Key bucket lru max size for one key bucket fd: {}, one inner length: {}， compress ratio maybe: {}, memory require: {}MB, name: {}",
                    maxSize, oneInnerLength, compressRatio, lruMemoryRequireMB, name);
            log.info("LRU prepare, type: {}, MB: {}, fd: {}", LRUPrepareBytesStats.Type.fd_key_bucket, lruMemoryRequireMB, name);
            LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_key_bucket, (int) lruMemoryRequireMB, false);

            if (maxSize > 0) {
                this.segmentBytesByIndexLRU = new LRUMap<>(maxSize);
                this.isLRUOn = true;
            }
        }

        if (ConfForSlot.global.pureMemory) {
            if (isChunkFd) {
                var segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
                this.allBytesByOneInnerIndex = new byte[segmentNumberPerFd][];
            } else {
                var bucketsPerSlotIgnoreFd = ConfForSlot.global.confBucket.bucketsPerSlot;
                this.allBytesByOneInnerIndex = new byte[bucketsPerSlotIgnoreFd][];
            }
        } else {
            var pageManager = PageManager.getInstance();
            var m = MemoryIO.getInstance();

            var npagesOneInner = oneInnerLength / PAGE_SIZE;

            this.readPageAddress = pageManager.allocatePages(npagesOneInner, PROTECTION);
            this.readPageBuffer = m.newDirectByteBuffer(readPageAddress, oneInnerLength);

            this.writePageAddress = pageManager.allocatePages(npagesOneInner, PROTECTION);
            this.writePageBuffer = m.newDirectByteBuffer(writePageAddress, oneInnerLength);

            this.writePageAddressB = pageManager.allocatePages(npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_PWRITE, PROTECTION);
            this.writePageBufferB = m.newDirectByteBuffer(writePageAddressB, oneInnerLength * BATCH_ONCE_SEGMENT_COUNT_PWRITE);

            var npagesRepl = npagesOneInner * ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT;
            this.readForReplAddress = pageManager.allocatePages(npagesRepl, PROTECTION);
            this.readForReplBuffer = m.newDirectByteBuffer(readForReplAddress, npagesRepl * PAGE_SIZE);

            this.writeForReplAddress = pageManager.allocatePages(npagesRepl, PROTECTION);
            this.writeForReplBuffer = m.newDirectByteBuffer(writeForReplAddress, npagesRepl * PAGE_SIZE);

            if (isChunkFd) {
                // only merge worker need read for merging batch
                int npagesMerge = npagesOneInner * MERGE_READ_ONCE_SEGMENT_COUNT;
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
        var npagesOneInner = oneInnerLength / PAGE_SIZE;

        var pageManager = PageManager.getInstance();
        if (readPageAddress != 0) {
            pageManager.freePages(readPageAddress, npagesOneInner);
            System.out.println("Clean up fd read, name: " + name + ", read page address: " + readPageAddress);

            readPageAddress = 0;
            readPageBuffer = null;
        }

        if (writePageAddress != 0) {
            pageManager.freePages(writePageAddress, npagesOneInner);
            System.out.println("Clean up fd write, name: " + name + ", write page address: " + writePageAddress);

            writePageAddress = 0;
            writePageBuffer = null;
        }

        if (writePageAddressB != 0) {
            pageManager.freePages(writePageAddressB, npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_PWRITE);
            System.out.println("Clean up fd write batch, name: " + name + ", write page address: " + writePageAddress);

            writePageAddressB = 0;
            writePageBufferB = null;
        }

        var npagesRepl = npagesOneInner * ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT;
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

        int npagesMerge = npagesOneInner * MERGE_READ_ONCE_SEGMENT_COUNT;
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

    private void checkOneInnerIndex(int oneInnerIndex) {
        if (isChunkFd) {
            // one inner index -> chunk segment index
            assert oneInnerIndex >= 0 && oneInnerIndex < ConfForSlot.global.confChunk.segmentNumberPerFd;
        } else {
            // one inner index -> bucket index
            assert oneInnerIndex >= 0 && oneInnerIndex < ConfForSlot.global.confBucket.bucketsPerSlot;
        }
    }

    private byte[] readInnerNotPureMemory(int oneInnerIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache) {
        return readInnerNotPureMemory(oneInnerIndex, buffer, callback, isRefreshLRUCache, buffer.capacity());
    }

    private byte[] readInnerNotPureMemory(int oneInnerIndex, ByteBuffer buffer, ReadBufferCallback callback, boolean isRefreshLRUCache, int length) {
        checkOneInnerIndex(oneInnerIndex);
        if (length > buffer.capacity()) {
            throw new IllegalArgumentException("Read length must be less than buffer capacity: " + buffer.capacity() + ", read length: " + length);
        }

        // for from lru cache if only read one segment
        var isOnlyOneOneInner = length == oneInnerLength;
        int oneInnerCount;
        if (isOnlyOneOneInner) {
            oneInnerCount = 1;

            if (isLRUOn) {
                var bytesCached = segmentBytesByIndexLRU.get(oneInnerIndex);
                if (bytesCached != null) {
                    lruHitCounter++;

                    if (isChunkFd) {
                        return bytesCached;
                    } else {
                        // only key bucket data need decompress
                        var beginT = System.nanoTime();
                        var bytesDecompressedCached = Zstd.decompress(bytesCached, oneInnerLength);
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
            oneInnerCount = length / oneInnerLength;
        }

        var offset = oneInnerIndex * oneInnerLength;
        if (offset >= writeIndex) {
            return null;
        }

        int readLength = length;
        var lastSegmentOffset = offset + (oneInnerCount * oneInnerLength);
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

        if (isOnlyOneOneInner && isRefreshLRUCache) {
            if (isLRUOn) {
                if (isChunkFd) {
                    segmentBytesByIndexLRU.put(oneInnerIndex, bytesRead);
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
                    segmentBytesByIndexLRU.put(oneInnerIndex, bytesCompressed);
                }
            }
        }
        return bytesRead;
    }

    private int writeInnerNotPureMemory(int oneInnerIndex, ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        checkOneInnerIndex(oneInnerIndex);

        int capacity = buffer.capacity();
        var oneInnerCount = capacity / oneInnerLength;
        var isOnlyOneSegment = oneInnerCount == 1;
        var isPwriteBatch = oneInnerCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE;

        int offset = oneInnerIndex * oneInnerLength;
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
                for (int i = 0; i < oneInnerCount; i++) {
                    var bytes = new byte[oneInnerLength];
                    buffer.position(i * oneInnerLength);
                    buffer.get(bytes);

                    // chunk data is already compressed
                    // key bucket data need not compress here, compress when after read
                    if (isChunkFd) {
                        segmentBytesByIndexLRU.put(oneInnerIndex + i, bytes);
                    }
                }
            } else {
                for (int i = 0; i < oneInnerCount; i++) {
                    segmentBytesByIndexLRU.remove(oneInnerIndex + i);
                }
            }
        }

        return n;
    }

    private byte[] readOneInnerBatchFromMemory(int oneInnerIndex, int oneInnerCount) {
        var bytesRead = new byte[oneInnerLength * oneInnerCount];
        for (int i = 0; i < oneInnerCount; i++) {
            var oneInnerBytes = allBytesByOneInnerIndex[oneInnerIndex + i];
            if (oneInnerBytes != null) {
                System.arraycopy(oneInnerBytes, 0, bytesRead, i * oneInnerLength, oneInnerBytes.length);
            }
        }
        return bytesRead;
    }

    public byte[] readOneInner(int oneInnerIndex, boolean isRefreshLRUCache) {
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(oneInnerIndex, 1);
        }

        return readInnerNotPureMemory(oneInnerIndex, readPageBuffer, null, isRefreshLRUCache);
    }

    public byte[] readSegmentForMerge(int segmentIndex, int segmentCount) {
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(segmentIndex, segmentCount);
        }

        return readInnerNotPureMemory(segmentIndex, readForMergeBatchBuffer, null, false, segmentCount * oneInnerLength);
    }

    public byte[] readOneInnerForRepl(int oneInnerIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(oneInnerIndex, ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT);
        }

        return readInnerNotPureMemory(oneInnerIndex, readForReplBuffer, null, false);
    }

    public byte[] readOneInnerForKeyBucketsInOneWalGroup(int bucketIndex) {
        // bucketIndex is oneInnerIndex when fd is key bucket fd, not chunk fd
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(bucketIndex, ConfForSlot.global.confWal.oneChargeBucketNumber);
        }

        return readInnerNotPureMemory(bucketIndex, readForOneWalGroupBatchBuffer, null, false);
    }

    public int clearOneOneInnerToMemory(int bucketIndex) {
        allBytesByOneInnerIndex[bucketIndex] = null;
        return 0;
    }

    public int writeOneInnerBatchToMemory(int beginBucketIndex, byte[] bytes, int position) {
        var isSmallerThanOneInner = bytes.length < oneInnerLength;
        if (!isSmallerThanOneInner && (bytes.length - position) % oneInnerLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of one inner length");
        }

        if (isSmallerThanOneInner) {
            // position is 0
            allBytesByOneInnerIndex[beginBucketIndex] = bytes;
            return bytes.length;
        }

        var oneInnerCount = (bytes.length - position) / oneInnerLength;
        var offset = position;
        for (int i = 0; i < oneInnerCount; i++) {
            var bytesOneSegment = new byte[oneInnerLength];
            System.arraycopy(bytes, offset, bytesOneSegment, 0, oneInnerLength);
            offset += oneInnerLength;

            allBytesByOneInnerIndex[beginBucketIndex + i] = bytesOneSegment;
        }
        return bytes.length;
    }

    public int writeOneInner(int oneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > oneInnerLength) {
            throw new IllegalArgumentException("Write bytes length must be less than one inner length");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeOneInnerBatchToMemory(oneInnerIndex, bytes, 0);
        }

        return writeInnerNotPureMemory(oneInnerIndex, writePageBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeSegmentBatch(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / oneInnerLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            final int position = 0;
            return writeOneInnerBatchToMemory(segmentIndex, bytes, position);
        }

        return writeInnerNotPureMemory(segmentIndex, writePageBufferB, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeOneInnerForKeyBucketsInOneWalGroup(int bucketIndex, byte[] bytes) {
        var keyBucketCount = bytes.length / oneInnerLength;
        if (keyBucketCount != ConfForSlot.global.confWal.oneChargeBucketNumber) {
            throw new IllegalArgumentException("Batch write bytes length not match one charge bucket number in one wal group");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeOneInnerBatchToMemory(bucketIndex, bytes, 0);
        }

        return writeInnerNotPureMemory(bucketIndex, writeForOneWalGroupBatchBuffer, (buffer) -> {
            buffer.clear();
            buffer.put(bytes);
        }, false);
    }

    public int writeOneInnerForRepl(int oneInnerIndex, byte[] bytes, int position) {
        var oneInnerCount = bytes.length / oneInnerLength;
        if (oneInnerCount != ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT) {
            throw new IllegalArgumentException("Repl write bytes length not match once repl segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeOneInnerBatchToMemory(oneInnerIndex, bytes, position);
        }

        return writeInnerNotPureMemory(oneInnerIndex, writeForReplBuffer, (buffer) -> {
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
