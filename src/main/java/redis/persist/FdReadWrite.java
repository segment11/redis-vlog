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
import redis.StaticMemoryPrepareBytesStats;
import redis.metric.SimpleGauge;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

// need thread safe
// need refactor to FdChunk + FdKeyBucket, todo
public class FdReadWrite {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    public FdReadWrite(String name, LibC libC, File file) throws IOException {
        this.name = name;
        if (!ConfForSlot.global.pureMemory) {
            if (!file.exists()) {
                FileUtils.touch(file);
            }
            this.libC = libC;
            this.fd = libC.open(file.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);
            this.writeIndex = file.length();
            log.info("Opened fd: {}, name: {}, file length: {}MB", fd, name, this.writeIndex / 1024 / 1024);
        } else {
            this.libC = null;
            this.fd = 0;
            log.warn("Pure memory mode, not use fd, name: {}", name);
        }
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
                double avgUs = (double) afterFdPreadCompressTimeTotalUs / afterFdPreadCompressCountTotal;
                map.put("after_fd_pread_compress_time_avg_us", new SimpleGauge.ValueWithLabelValues(avgUs, labelValues));
            }

            if (keyBucketSharedBytesCompressCountTotal > 0) {
                map.put("key_bucket_shared_bytes_compress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) keyBucketSharedBytesCompressTimeTotalUs, labelValues));
                map.put("key_bucket_shared_bytes_compress_count_total", new SimpleGauge.ValueWithLabelValues((double) keyBucketSharedBytesCompressCountTotal, labelValues));
                double avgUs = (double) keyBucketSharedBytesCompressTimeTotalUs / keyBucketSharedBytesCompressCountTotal;
                map.put("key_bucket_shared_bytes_compress_time_avg_us", new SimpleGauge.ValueWithLabelValues(avgUs, labelValues));

                // compress ratio
                double compressRatio = (double) keyBucketSharedBytesAfterCompressedBytesTotal / keyBucketSharedBytesBeforeCompressedBytesTotal;
                map.put("key_bucket_shared_bytes_compress_ratio", new SimpleGauge.ValueWithLabelValues(compressRatio, labelValues));
            }

            if (keyBucketSharedBytesDecompressCountTotal > 0) {
                map.put("key_bucket_shared_bytes_decompress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) keyBucketSharedBytesDecompressTimeTotalUs, labelValues));
                map.put("key_bucket_shared_bytes_decompress_count_total", new SimpleGauge.ValueWithLabelValues((double) keyBucketSharedBytesDecompressCountTotal, labelValues));
                double avgUs = (double) keyBucketSharedBytesDecompressTimeTotalUs / keyBucketSharedBytesDecompressCountTotal;
                map.put("key_bucket_shared_bytes_decompress_time_avg_us", new SimpleGauge.ValueWithLabelValues(avgUs, labelValues));
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

    @Override
    public String toString() {
        return "FdReadWrite{" +
                "name='" + name + '\'' +
                ", fd=" + fd +
                ", writeIndex=" + writeIndex +
                ", isLRUOn=" + isLRUOn +
                ", isChunkFd=" + isChunkFd +
                ", oneInnerLength=" + oneInnerLength +
                '}';
    }

    // for metrics
    final static SimpleGauge fdReadWriteGauge = new SimpleGauge("fd_read_write", "chunk or key buckets file read write",
            "name");

    static {
        fdReadWriteGauge.register();
    }

    // metric stats
    // avg = this time / cache hit count
    private long afterLRUReadDecompressTimeTotalUs;

    // avg = this time / after fd pread compress count total
    private long afterFdPreadCompressTimeTotalUs;
    long afterFdPreadCompressCountTotal;

    private long readBytesTotal;
    private long readTimeTotalUs;
    long readCountTotal;

    private long writeBytesTotal;
    private long writeTimeTotalUs;
    long writeCountTotal;

    long lruHitCounter;
    long lruMissCounter;

    // only for pure memory mode stats
    private long keyBucketSharedBytesCompressTimeTotalUs;
    long keyBucketSharedBytesCompressCountTotal;
    long keyBucketSharedBytesBeforeCompressedBytesTotal;
    long keyBucketSharedBytesAfterCompressedBytesTotal;


    private long keyBucketSharedBytesDecompressTimeTotalUs;
    long keyBucketSharedBytesDecompressCountTotal;

    public static final int BATCH_ONCE_SEGMENT_COUNT_PWRITE = 4;
    public static final int REPL_ONCE_SEGMENT_COUNT_PREAD = 1024;

    private ByteBuffer oneInnerBuffer;
    private long oneInnerAddress;

    // for chunk batch write segments
    private ByteBuffer writeSegmentBatchBuffer;
    private long writeSegmentBatchAddress;

    // for chunk segments repl
    ByteBuffer forReplBuffer;
    private long forReplAddress;

    // for chunk merge
    ByteBuffer readForMergeBatchBuffer;
    private long readForMergeBatchAddress;

    // for key bucket batch read / write
    ByteBuffer forOneWalGroupBatchBuffer;
    private long forOneWalGroupBatchAddress;

    private int oneInnerLength;
    boolean isChunkFd;

    static final int BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE = 10;

    // for chunk, already compressed, refer to SegmentBatch
    // first index is relative segment index in target chunk fd, !!!important, relative segment index
    private byte[][] allBytesBySegmentIndexForOneChunkFd;
    // for key bucket, compressed, need compress before set here and decompress after read from here
    private byte[][] allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex;

    void resetAllBytesByOneWalGroupIndexForKeyBucketOneSplitIndexForTest(int walGroupNumber) {
        this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex = new byte[walGroupNumber][];
    }

    private void setSharedBytesCompressToMemory(byte[] sharedBytes, int walGroupIndex) {
        if (sharedBytes == null) {
            allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex] = null;
            return;
        }

        // tips: one wal group may charge 32 key buckets, = 32 * 4K = 128K, compress cost may take 500us
        // one wal group charge 16 key buckets will perform better
        var beginT = System.nanoTime();
        var sharedBytesCompressed = Zstd.compress(sharedBytes);
        var costT = (System.nanoTime() - beginT) / 1000;

        allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex] = sharedBytesCompressed;

        // stats
        keyBucketSharedBytesCompressTimeTotalUs += costT;
        keyBucketSharedBytesCompressCountTotal++;
        keyBucketSharedBytesBeforeCompressedBytesTotal += sharedBytes.length;
        keyBucketSharedBytesAfterCompressedBytesTotal += sharedBytesCompressed.length;
    }

    private byte[] getSharedBytesDecompressFromMemory(int walGroupIndex) {
        var compressedSharedBytes = allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex[walGroupIndex];
        if (compressedSharedBytes == null) {
            return null;
        }

        // tips: one wal group may charge 32 key buckets, = 32 * 4K = 128K, decompress cost may take 200-300us
        // one wal group charge 16 key buckets will perform better
        var sharedBytes = new byte[ConfForSlot.global.confWal.oneChargeBucketNumber * oneInnerLength];
        var beginT = System.nanoTime();
        Zstd.decompress(sharedBytes, compressedSharedBytes);
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        keyBucketSharedBytesDecompressTimeTotalUs += costT;
        keyBucketSharedBytesDecompressCountTotal++;
        return sharedBytes;
    }

    public boolean isTargetSegmentIndexNullInMemory(int segmentIndex) {
        return allBytesBySegmentIndexForOneChunkFd[segmentIndex] == null;
    }

    // chunk fd is by segment index, key bucket fd is by bucket index
    private LRUMap<Integer, byte[]> oneInnerBytesByIndexLRU;

    public void initByteBuffers(boolean isChunkFd) {
        var oneInnerLength = isChunkFd ? ConfForSlot.global.confChunk.segmentLength : KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
        this.oneInnerLength = oneInnerLength;
        this.isChunkFd = isChunkFd;

        initLRU(isChunkFd, oneInnerLength);

        if (ConfForSlot.global.pureMemory) {
            if (isChunkFd) {
                var segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
                this.allBytesBySegmentIndexForOneChunkFd = new byte[segmentNumberPerFd][];
            } else {
                var walGroupNumber = Wal.calcWalGroupNumber();
                this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex = new byte[walGroupNumber][];
            }
        } else {
            long initMemoryN = 0;

            var pageManager = PageManager.getInstance();
            var m = MemoryIO.getInstance();

            var npagesOneInner = oneInnerLength / PAGE_SIZE;

            this.oneInnerAddress = pageManager.allocatePages(npagesOneInner, PROTECTION);
            this.oneInnerBuffer = m.newDirectByteBuffer(oneInnerAddress, oneInnerLength);

            initMemoryN += oneInnerBuffer.capacity();

            if (isChunkFd) {
                // chunk write segment batch
                var npagesWriteSegmentBatch = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_PWRITE;
                this.writeSegmentBatchAddress = pageManager.allocatePages(npagesWriteSegmentBatch, PROTECTION);
                this.writeSegmentBatchBuffer = m.newDirectByteBuffer(writeSegmentBatchAddress, npagesWriteSegmentBatch * PAGE_SIZE);

                var npagesRepl = npagesOneInner * REPL_ONCE_SEGMENT_COUNT_PREAD;
                this.forReplAddress = pageManager.allocatePages(npagesRepl, PROTECTION);
                this.forReplBuffer = m.newDirectByteBuffer(forReplAddress, npagesRepl * PAGE_SIZE);

                // only merge worker need read for merging batch
                int npagesMerge = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
                this.readForMergeBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
                this.readForMergeBatchBuffer = m.newDirectByteBuffer(readForMergeBatchAddress, npagesMerge * PAGE_SIZE);

                initMemoryN += writeSegmentBatchBuffer.capacity() + forReplBuffer.capacity() + readForMergeBatchBuffer.capacity();
            } else {
                int npagesOneWalGroup = ConfForSlot.global.confWal.oneChargeBucketNumber;
                this.forOneWalGroupBatchAddress = pageManager.allocatePages(npagesOneWalGroup, PROTECTION);
                this.forOneWalGroupBatchBuffer = m.newDirectByteBuffer(forOneWalGroupBatchAddress, npagesOneWalGroup * PAGE_SIZE);

                initMemoryN += forOneWalGroupBatchBuffer.capacity();
            }

            int initMemoryMB = (int) (initMemoryN / 1024 / 1024);
            log.info("Static memory init, type: {}, MB: {}, name: {}", StaticMemoryPrepareBytesStats.Type.fd_read_write_buffer, initMemoryMB, name);
            StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.fd_read_write_buffer, initMemoryMB, false);
        }
    }

    private void initLRU(boolean isChunkFd, int oneInnerLength) {
        if (!ConfForSlot.global.pureMemory) {
            if (isChunkFd) {
                var maxSize = ConfForSlot.global.confChunk.lruPerFd.maxSize;
                var lruMemoryRequireMB = ((long) maxSize * oneInnerLength) / 1024 / 1024;
                log.info("Chunk lru max size for one chunk fd: {}, one inner length: {}, memory require: {}MB, name: {}",
                        maxSize, oneInnerLength, lruMemoryRequireMB, name);
                log.info("LRU prepare, type: {}, MB: {}, fd: {}", LRUPrepareBytesStats.Type.fd_chunk_data, lruMemoryRequireMB, name);
                LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_chunk_data, (int) lruMemoryRequireMB, true);

                if (maxSize > 0) {
                    this.oneInnerBytesByIndexLRU = new LRUMap<>(maxSize);
                    this.isLRUOn = true;
                }
            } else {
                // key bucket
                var maxSize = ConfForSlot.global.confBucket.lruPerFd.maxSize;
                // need to compare with metrics
                final var compressRatio = 0.25;
                var lruMemoryRequireMB = ((long) maxSize * oneInnerLength) / 1024 / 1024 * compressRatio;
                log.info("Key bucket lru max size for one key bucket fd: {}, one inner length: {}， compress ratio maybe: {}, memory require: {}MB, name: {}",
                        maxSize, oneInnerLength, compressRatio, lruMemoryRequireMB, name);
                log.info("LRU prepare, type: {}, MB: {}, fd: {}", LRUPrepareBytesStats.Type.fd_key_bucket, lruMemoryRequireMB, name);
                LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_key_bucket, (int) lruMemoryRequireMB, false);

                if (maxSize > 0) {
                    this.oneInnerBytesByIndexLRU = new LRUMap<>(maxSize);
                    this.isLRUOn = true;
                }
            }
        } else {
            log.warn("Pure memory mode, not use lru cache, name: {}", name);
        }
    }

    public void cleanUp() {
        var npagesOneInner = oneInnerLength / PAGE_SIZE;

        var pageManager = PageManager.getInstance();
        if (oneInnerAddress != 0) {
            pageManager.freePages(oneInnerAddress, npagesOneInner);
            System.out.println("Clean up fd read, name: " + name + ", one inner address: " + oneInnerAddress);

            oneInnerAddress = 0;
            oneInnerBuffer = null;
        }

        if (writeSegmentBatchAddress != 0) {
            var npagesWriteSegmentBatch = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_PWRITE;
            pageManager.freePages(writeSegmentBatchAddress, npagesWriteSegmentBatch);
            System.out.println("Clean up fd write, name: " + name + ", write segment batch address: " + writeSegmentBatchAddress);

            writeSegmentBatchAddress = 0;
            writeSegmentBatchBuffer = null;
        }

        if (forReplAddress != 0) {
            var npagesRepl = npagesOneInner * REPL_ONCE_SEGMENT_COUNT_PREAD;
            pageManager.freePages(forReplAddress, npagesRepl);
            System.out.println("Clean up fd read, name: " + name + ", for repl address: " + forReplAddress);

            forReplAddress = 0;
            forReplBuffer = null;
        }

        if (readForMergeBatchAddress != 0) {
            var npagesMerge = npagesOneInner * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
            pageManager.freePages(readForMergeBatchAddress, npagesMerge);
            System.out.println("Clean up fd read, name: " + name + ", read for merge batch address: " + readForMergeBatchAddress);

            readForMergeBatchAddress = 0;
            readForMergeBatchBuffer = null;
        }

        if (forOneWalGroupBatchAddress != 0) {
            var npagesOneWalGroup = ConfForSlot.global.confWal.oneChargeBucketNumber;
            pageManager.freePages(forOneWalGroupBatchAddress, npagesOneWalGroup);
            System.out.println("Clean up fd read, name: " + name + ", for one wal group address: " + forOneWalGroupBatchAddress);

            forOneWalGroupBatchAddress = 0;
            forOneWalGroupBatchBuffer = null;
        }

        if (fd != 0) {
            int r = libC.close(fd);
            if (r < 0) {
                System.err.println("Close fd error: " + libC.strerror(r) + ", name: " + name);
            }
            System.out.println("Closed fd: " + fd + ", name: " + name);
        }
    }

    interface WriteBufferPrepare {
        void prepare(ByteBuffer buffer);
    }

    private void checkOneInnerIndex(int oneInnerIndex) {
        if (isChunkFd) {
            // one inner index -> chunk segment index
            if (oneInnerIndex < 0 || oneInnerIndex >= ConfForSlot.global.confChunk.segmentNumberPerFd) {
                throw new IllegalArgumentException("Segment index: " + oneInnerIndex + " must be in [0, " + ConfForSlot.global.confChunk.segmentNumberPerFd + ")");
            }
        } else {
            // one inner index -> bucket index
            if (oneInnerIndex < 0 || oneInnerIndex >= ConfForSlot.global.confBucket.bucketsPerSlot) {
                throw new IllegalArgumentException("Bucket index: " + oneInnerIndex + " must be in [0, " + ConfForSlot.global.confBucket.bucketsPerSlot + ")");
            }
        }
    }

    private byte[] readInnerByBuffer(int oneInnerIndex, ByteBuffer buffer, boolean isRefreshLRUCache) {
        return readInnerByBuffer(oneInnerIndex, buffer, isRefreshLRUCache, buffer.capacity());
    }

    private byte[] readInnerByBuffer(int oneInnerIndex, ByteBuffer buffer, boolean isRefreshLRUCache, int length) {
        checkOneInnerIndex(oneInnerIndex);
        if (length > buffer.capacity()) {
            throw new IllegalArgumentException("Read length must be less than buffer capacity: " + buffer.capacity() + ", read length: " + length + ", name: " + name);
        }

        // for from lru cache if only read one segment
        var isOnlyOneOneInner = length == oneInnerLength;
        int oneInnerCount;
        if (isOnlyOneOneInner) {
            oneInnerCount = 1;

            if (isLRUOn) {
                var bytesCached = oneInnerBytesByIndexLRU.get(oneInnerIndex);
                if (bytesCached != null) {
                    lruHitCounter++;

                    if (isChunkFd) {
                        return bytesCached;
                    } else {
                        // only key bucket data need decompress
                        var beginT = System.nanoTime();
                        var bytesDecompressedCached = Zstd.decompress(bytesCached, oneInnerLength);
                        var costT = (System.nanoTime() - beginT) / 1000;
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
        if (readLength < 0) {
            throw new IllegalArgumentException("Read length must be greater than 0, read length: " + readLength);
        }
        if (readLength > length) {
            throw new IllegalArgumentException("Read length must be less than given length: " + length + ", read length: " + readLength);
        }

        // clear buffer before read
        buffer.clear();

        readCountTotal++;
        var beginT = System.nanoTime();
        var n = libC.pread(fd, buffer, readLength, offset);
        var costT = (System.nanoTime() - beginT) / 1000;
        readTimeTotalUs += costT;
        readBytesTotal += n;

        if (n != readLength) {
            log.error("Read error, n: {}, read length: {}, name: {}", n, readLength, name);
            throw new RuntimeException("Read error, n: " + n + ", read length: " + readLength + ", name: " + name);
        }

        buffer.rewind();
        var bytesRead = new byte[readLength];
        buffer.get(bytesRead);

        if (isOnlyOneOneInner && isRefreshLRUCache) {
            if (isLRUOn) {
                if (isChunkFd) {
                    oneInnerBytesByIndexLRU.put(oneInnerIndex, bytesRead);
                } else {
                    // only key bucket data need compress
                    var beginT2 = System.nanoTime();
                    var bytesCompressed = Zstd.compress(bytesRead);
                    var costT2 = (System.nanoTime() - beginT2) / 1000;
                    afterFdPreadCompressTimeTotalUs += costT2;
                    afterFdPreadCompressCountTotal++;
                    oneInnerBytesByIndexLRU.put(oneInnerIndex, bytesCompressed);
                }
            }
        }
        return bytesRead;
    }

    private int writeInnerByBuffer(int oneInnerIndex, ByteBuffer buffer, @NotNull WriteBufferPrepare prepare, boolean isRefreshLRUCache) {
        checkOneInnerIndex(oneInnerIndex);

        int capacity = buffer.capacity();
        var oneInnerCount = capacity / oneInnerLength;
        var isOnlyOneSegment = oneInnerCount == 1;
        var isPwriteBatch = oneInnerCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE;

        int offset = oneInnerIndex * oneInnerLength;

        // clear buffer before write
        buffer.clear();

        prepare.prepare(buffer);
        // when write bytes length < capacity, pending 0
        if (buffer.position() < capacity) {
            // pending 0
            var bytes0 = new byte[capacity - buffer.position()];
            buffer.put(bytes0);
        }
        buffer.rewind();

        writeCountTotal++;
        var beginT = System.nanoTime();
        var n = libC.pwrite(fd, buffer, capacity, offset);
        var costT = (System.nanoTime() - beginT) / 1000;
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
            if (isRefreshLRUCache && isChunkFd && (isOnlyOneSegment || isPwriteBatch)) {
                for (int i = 0; i < oneInnerCount; i++) {
                    var bytes = new byte[oneInnerLength];
                    buffer.position(i * oneInnerLength);
                    buffer.get(bytes);

                    // chunk data is already compressed
                    oneInnerBytesByIndexLRU.put(oneInnerIndex + i, bytes);
                }
            } else {
                for (int i = 0; i < oneInnerCount; i++) {
                    oneInnerBytesByIndexLRU.remove(oneInnerIndex + i);
                }
            }
        }

        return n;
    }

    byte[] readOneInnerBatchFromMemory(int oneInnerIndex, int oneInnerCount) {
        if (!isChunkFd) {
            // read shared bytes for one wal group
            // oneInnerIndex -> beginBucketIndex
            var walGroupIndex = Wal.calWalGroupIndex(oneInnerIndex);
            if (oneInnerCount == 1 || oneInnerCount == ConfForSlot.global.confWal.oneChargeBucketNumber) {
                // readonly, use a copy will be safe, but not necessary
                return getSharedBytesDecompressFromMemory(walGroupIndex);
            } else {
                throw new IllegalArgumentException("Read error, key loader fd once read key buckets count invalid: " + oneInnerCount);
            }
        }

        // bellow for chunk fd
        if (oneInnerCount == 1) {
            return allBytesBySegmentIndexForOneChunkFd[oneInnerIndex];
        }

        var bytesRead = new byte[oneInnerLength * oneInnerCount];
        for (int i = 0; i < oneInnerCount; i++) {
            var oneInnerBytes = allBytesBySegmentIndexForOneChunkFd[oneInnerIndex + i];
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

        return readInnerByBuffer(oneInnerIndex, oneInnerBuffer, isRefreshLRUCache);
    }

    // segmentCount may < BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE, when one slot read segments in the same wal group before batch update key buckets
    public byte[] readSegmentsForMerge(int beginSegmentIndex, int segmentCount) {
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(beginSegmentIndex, segmentCount);
        }

        return readInnerByBuffer(beginSegmentIndex, readForMergeBatchBuffer, false, segmentCount * oneInnerLength);
    }

    public byte[] readBatchForRepl(int oneInnerIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(oneInnerIndex, REPL_ONCE_SEGMENT_COUNT_PREAD);
        }

        return readInnerByBuffer(oneInnerIndex, forReplBuffer, false);
    }

    public byte[] readKeyBucketsSharedBytesInOneWalGroup(int beginBucketIndex) {
        if (ConfForSlot.global.pureMemory) {
            return readOneInnerBatchFromMemory(beginBucketIndex, ConfForSlot.global.confWal.oneChargeBucketNumber);
        }

        return readInnerByBuffer(beginBucketIndex, forOneWalGroupBatchBuffer, false);
    }

    public void clearOneKeyBucketToMemoryForTest(int bucketIndex) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var sharedBytes = getSharedBytesDecompressFromMemory(walGroupIndex);
        if (sharedBytes == null) {
            return;
        }

        var position = KeyLoader.getPositionInSharedBytes(bucketIndex);
        // set 0
        Arrays.fill(sharedBytes, position, position + oneInnerLength, (byte) 0);
        // compress and set back
        setSharedBytesCompressToMemory(sharedBytes, walGroupIndex);
    }

    public void clearKeyBucketsInOneWalGroupToMemory(int bucketIndex) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        setSharedBytesCompressToMemory(null, walGroupIndex);
    }

    public void clearOneWalGroupToMemoryForTest(int walGroupIndex) {
        setSharedBytesCompressToMemory(null, walGroupIndex);
    }

    int writeOneInnerBatchToMemory(int beginOneInnerIndex, byte[] bytes, int position) {
        var isSmallerThanOneInner = bytes.length < oneInnerLength;
        if (!isSmallerThanOneInner && (bytes.length - position) % oneInnerLength != 0) {
            throw new IllegalArgumentException("Bytes length must be multiple of one inner length");
        }

        if (isSmallerThanOneInner) {
            // always is chunk fd
            if (!isChunkFd) {
                throw new IllegalStateException("Write bytes smaller than one segment length to memory must be chunk fd");
            }
            // position is 0
            allBytesBySegmentIndexForOneChunkFd[beginOneInnerIndex] = bytes;
            return bytes.length;
        }

        var oneInnerCount = (bytes.length - position) / oneInnerLength;
        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        // for key bucket, memory copy one wal group by one wal group
        if (!isChunkFd) {
            if (oneInnerCount != 1) {
                // oneInnerCount == oenChargeBucketNumber, is in another method
                throw new IllegalArgumentException("Write key bucket once by one wal group or only one key bucket");
            }
        }

        // memory copy one segment/bucket by one segment/bucket
        var offset = position;
        for (int i = 0; i < oneInnerCount; i++) {
            var targetOneInnerIndex = beginOneInnerIndex + i;
            if (!isChunkFd) {
                var walGroupIndex = Wal.calWalGroupIndex(targetOneInnerIndex);
                var oneWalGroupSharedBytesLength = oneChargeBucketNumber * oneInnerLength;
                var sharedBytes = getSharedBytesDecompressFromMemory(walGroupIndex);
                if (sharedBytes == null) {
                    sharedBytes = new byte[oneWalGroupSharedBytesLength];
                }
                var targetBucketIndexPosition = KeyLoader.getPositionInSharedBytes(targetOneInnerIndex);
                System.arraycopy(bytes, offset, sharedBytes, targetBucketIndexPosition, oneInnerLength);
                setSharedBytesCompressToMemory(sharedBytes, walGroupIndex);
            } else {
                var bytesOneSegment = new byte[oneInnerLength];
                System.arraycopy(bytes, offset, bytesOneSegment, 0, oneInnerLength);
                allBytesBySegmentIndexForOneChunkFd[targetOneInnerIndex] = bytesOneSegment;
            }
            offset += oneInnerLength;
        }
        return oneInnerCount * oneInnerLength;
    }

    public int writeOneInner(int oneInnerIndex, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > oneInnerLength) {
            throw new IllegalArgumentException("Write bytes length must be less than one inner length");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeOneInnerBatchToMemory(oneInnerIndex, bytes, 0);
        }

        return writeInnerByBuffer(oneInnerIndex, oneInnerBuffer, (buffer) -> {
            // buffer already clear
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeSegmentsBatch(int segmentIndex, byte[] bytes, boolean isRefreshLRUCache) {
        var segmentCount = bytes.length / oneInnerLength;
        if (segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            throw new IllegalArgumentException("Batch write bytes length not match once batch write segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeOneInnerBatchToMemory(segmentIndex, bytes, 0);
        }

        return writeInnerByBuffer(segmentIndex, writeSegmentBatchBuffer, (buffer) -> {
            // buffer already clear
            buffer.put(bytes);
        }, isRefreshLRUCache);
    }

    public int writeSharedBytesForKeyBucketsInOneWalGroup(int bucketIndex, byte[] sharedBytes) {
        var keyBucketCount = sharedBytes.length / oneInnerLength;
        if (keyBucketCount != ConfForSlot.global.confWal.oneChargeBucketNumber) {
            throw new IllegalArgumentException("Batch write bytes length not match one charge bucket number in one wal group");
        }

        if (ConfForSlot.global.pureMemory) {
            var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
            setSharedBytesCompressToMemory(sharedBytes, walGroupIndex);
            return sharedBytes.length;
        }

        return writeInnerByBuffer(bucketIndex, forOneWalGroupBatchBuffer, (buffer) -> buffer.put(sharedBytes), false);
    }

    public int writeSegmentBatchForRepl(int beginSegmentIndex, byte[] bytes, int position) {
        var segmentCount = bytes.length / oneInnerLength;
        if (segmentCount != REPL_ONCE_SEGMENT_COUNT_PREAD) {
            throw new IllegalArgumentException("Repl write segment batch bytes length not match repl once segment count");
        }

        if (ConfForSlot.global.pureMemory) {
            return writeOneInnerBatchToMemory(beginSegmentIndex, bytes, position);
        }

        return writeInnerByBuffer(beginSegmentIndex, forReplBuffer, (buffer) -> {
            if (position == 0) {
                buffer.put(bytes);
            } else {
                buffer.put(bytes, position, bytes.length - position);
            }
        }, false);
    }

    public void truncate() {
        if (libC != null) {
            var r = libC.ftruncate(fd, 0);
            if (r < 0) {
                throw new RuntimeException("Truncate error: " + libC.strerror(r));
            }
            log.info("Truncate fd: {}, name: {}", fd, name);

            if (isLRUOn) {
                oneInnerBytesByIndexLRU.clear();
                log.info("LRU cache clear, name: {}", name);
            }
        } else {
            log.warn("Pure memory mode, not use fd, name: {}", name);
            if (isChunkFd) {
                this.allBytesBySegmentIndexForOneChunkFd = new byte[allBytesBySegmentIndexForOneChunkFd.length][];
            } else {
                this.allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex = new byte[allBytesByOneWalGroupIndexForKeyBucketOneSplitIndex.length][];
            }
            log.info("Clear all bytes in memory, name: {}", name);
        }
    }
}
