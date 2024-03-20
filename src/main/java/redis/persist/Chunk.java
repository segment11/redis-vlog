package redis.persist;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.Debug;
import redis.SnowFlake;
import redis.repl.MasterUpdateCallback;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

public class Chunk implements OfStats {
    private final int maxSegmentNumberPerFd;
    private final byte maxFdPerChunk;
    private final long maxFdLength;
    private final int maxSegmentIndex;
    private final int halfSegmentIndex;

    // seq long + cv number int + crc int
    public static final int SEGMENT_HEADER_LENGTH = 8 + 4 + 4;
    public static final byte SEGMENT_FLAG_INIT = 100;
    public static final byte SEGMENT_FLAG_NEW = 0;
    public static final byte SEGMENT_FLAG_REUSE_AND_PERSISTED = 10;
    public static final byte SEGMENT_FLAG_MERGING = 1;
    public static final byte SEGMENT_FLAG_MERGED = -1;
    public static final byte SEGMENT_FLAG_MERGED_AND_PERSISTED = -10;
    public static final byte SEGMENT_FLAG_REUSE = -100;
    public static final byte MAIN_WORKER_ID = -1;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private boolean isRequestWorker;
    private boolean isMergeWorker;
    private boolean isTopMergeWorker;

    public void setWorkerType(boolean requestWorker, boolean mergeWorker, boolean topMergeWorker) {
        this.isRequestWorker = requestWorker;
        this.isMergeWorker = mergeWorker;
        this.isTopMergeWorker = topMergeWorker;
    }

    // only begin count after server start, not persist
    private int reuseLoopCount = 0;

    final byte workerId;
    final byte batchIndex;
    final byte slot;
    // for better latency, segment length = 4096 decompress performance is better
    private final int segmentLength;
    static final int MERGE_READ_SEGMENT_ONCE_COUNT = 10;
    private final File slotDir;

    private final SegmentBatch segmentBatch;

    // for merge
    ByteBuffer readForMergingBatchBuffer;
    private long readForMergingBatchAddress;

    // for top merge
    ByteBuffer readSelfForMergingBatchBuffer;
    private long readSelfForMergingBatchAddress;

    private final int BATCH_SEGMENT_COUNT_FOR_PWRITE = 4;

    private ByteBuffer writePageBuffer;
    private ByteBuffer writePageBufferB;
    private long writePageAddress;
    private long writePageAddressB;

    // stats
    private long persistCount;
    private long persistSegmentCount;
    private long persistCvCount;
    private long persistCostTotalTimeMicros = 0;
    private long updatePvmCostTotalTimeMicros = 0;

    private final OneSlot oneSlot;
    private final KeyLoader keyLoader;

    private LibC libC;

    private int[] fds;
    private int[] fdLengths;
    private ByteBuffer[] readSegmentBuffers;
    private long[] readSegmentBufferAddresses;

    private final MasterUpdateCallback masterUpdateCallback;

    public Chunk(byte workerId, byte slot, byte batchIndex, byte requestWorkers,
                 SnowFlake snowFlake, File slotDir, OneSlot oneSlot, KeyLoader keyLoader, MasterUpdateCallback masterUpdateCallback) {
        this.maxSegmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
        this.maxFdPerChunk = ConfForSlot.global.confChunk.fdPerChunk;
        this.maxFdLength = (long) maxSegmentNumberPerFd * ConfForSlot.global.confChunk.segmentLength;

        int maxSegmentNumber = maxSegmentNumberPerFd * maxFdPerChunk;
        this.maxSegmentIndex = maxSegmentNumber - 1;
        this.halfSegmentIndex = maxSegmentNumber / 2;

        this.workerId = workerId;
        this.batchIndex = batchIndex;
        this.slot = slot;
        this.segmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.snowFlake = snowFlake;
        this.slotDir = slotDir;
        this.oneSlot = oneSlot;
        this.keyLoader = keyLoader;
        this.masterUpdateCallback = masterUpdateCallback;

        this.segmentBatch = new SegmentBatch(workerId, slot, batchIndex, snowFlake);

        var pageManager = PageManager.getInstance();
        var m = MemoryIO.getInstance();

        var npages0 = segmentLength / PAGE_SIZE;
        this.writePageAddress = pageManager.allocatePages(npages0, PROTECTION);
        this.writePageBuffer = m.newDirectByteBuffer(writePageAddress, segmentLength);

        this.writePageAddressB = pageManager.allocatePages(npages0 * BATCH_SEGMENT_COUNT_FOR_PWRITE, PROTECTION);
        this.writePageBufferB = m.newDirectByteBuffer(writePageAddressB, segmentLength * BATCH_SEGMENT_COUNT_FOR_PWRITE);

        if (workerId >= requestWorkers) {
            int npagesMerge = npages0 * MERGE_READ_SEGMENT_ONCE_COUNT;

            // only merge worker need read for merging batch
            this.readForMergingBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
            this.readForMergingBatchBuffer = m.newDirectByteBuffer(readForMergingBatchAddress, npagesMerge * PAGE_SIZE);

            // only top merge worker need read self for merging batch
            this.readSelfForMergingBatchAddress = pageManager.allocatePages(npagesMerge, PROTECTION);
            this.readSelfForMergingBatchBuffer = m.newDirectByteBuffer(readSelfForMergingBatchAddress, npagesMerge * PAGE_SIZE);
        }
    }

    public void initFds(LibC libC) throws IOException {
        this.libC = libC;

        this.fds = new int[maxFdPerChunk];
        this.fdLengths = new int[maxFdPerChunk];
        // read segment when get
        this.readSegmentBuffers = new ByteBuffer[maxFdPerChunk];
        this.readSegmentBufferAddresses = new long[maxFdPerChunk];

        var chunkDir = new File(slotDir, "chunk-w-" + workerId + "-b-" + batchIndex);
        if (!chunkDir.exists()) {
            var isOk = chunkDir.mkdirs();
            if (!isOk) {
                throw new IOException("Create chunk dir error: " + chunkDir.getAbsolutePath());
            }
        }

        var pageManager = PageManager.getInstance();
        var m = MemoryIO.getInstance();

        var npages0 = segmentLength / PAGE_SIZE;

        for (int fdIndex = 0; fdIndex < maxFdPerChunk; fdIndex++) {
            var oneFile = new File(chunkDir, "fd-" + fdIndex + ".data");
            if (!oneFile.exists()) {
                FileUtils.touch(oneFile);
            }

            fds[fdIndex] = libC.open(oneFile.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);

            int fileLength = (int) oneFile.length();
            // align segment size
            if (fileLength % PAGE_SIZE != 0) {
                // will never happen
                log.error("File length page size not aligned, file: {}, length: {}", oneFile.getAbsoluteFile(), fileLength);
                fileLength = (fileLength / PAGE_SIZE + 1) * PAGE_SIZE;
            }
            fdLengths[fdIndex] = fileLength;

            var addr = pageManager.allocatePages(npages0, PROTECTION);
            readSegmentBufferAddresses[fdIndex] = addr;
            readSegmentBuffers[fdIndex] = m.newDirectByteBuffer(addr, segmentLength);
        }
    }

    int preadForMerge(int targetSegmentIndex, ByteBuffer buffer, long offset) {
        int fdIndex = targetFdIndex(targetSegmentIndex);
        int fd = fds[fdIndex];
        var fdOffset = (int) (offset % maxFdLength);
        return libC.pread(fd, buffer, buffer.capacity(), fdOffset);
    }

    private long preadCostNanos;

    private long preadCount;

    byte[] preadSegmentTightBytesWithLength(int targetSegmentIndex) {
        int fdIndex = targetFdIndex(targetSegmentIndex);
        int fd = fds[fdIndex];
        var readSegmentBuffer = readSegmentBuffers[fdIndex];

//        assert readSegmentBuffer.capacity() == segmentLength;
        long offset = (long) targetSegmentIndex * segmentLength;
        var fdOffset = (int) (offset % maxFdLength);

        // use more than one read byte buffer for one chunk may be better
        synchronized (readSegmentBuffer) {
            readSegmentBuffer.clear();

            long beginT = System.nanoTime();
            int n = libC.pread(fd, readSegmentBuffer, segmentLength, fdOffset);
            long costT = System.nanoTime() - beginT;
            if (n != segmentLength) {
                throw new IllegalStateException("Read persisted segment index error, fd: " + fd + ", offset: " + offset +
                        ", length: " + segmentLength + ", last error: " + libC.strerror(n));
            }
            readSegmentBuffer.rewind();

            preadCostNanos += costT;
            preadCount++;

            // memory copy
            // only need multi sub block compressed bytes with length, so the lru cache size is smaller
            int tightBytesLength = readSegmentBuffer.getInt();
            var tightBytesWithLength = new byte[4 + tightBytesLength];
            readSegmentBuffer.position(0).get(tightBytesWithLength);
            return tightBytesWithLength;
        }
    }

    public void cleanUp() {
        var npages0 = segmentLength / PAGE_SIZE;

        var pageManager = PageManager.getInstance();
        if (writePageAddress != 0) {
            pageManager.freePages(writePageAddress, npages0);
            System.out.println("Clean up chunk, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", write page address: " + writePageAddress);

            writePageAddress = 0;
            writePageBuffer = null;
        }

        if (writePageAddressB != 0) {
            pageManager.freePages(writePageAddressB, npages0 * BATCH_SEGMENT_COUNT_FOR_PWRITE);
            System.out.println("Clean up chunk, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", write page address batch: " + writePageAddressB);

            writePageAddressB = 0;
            writePageBufferB = null;
        }

        int npagesMerge = npages0 * MERGE_READ_SEGMENT_ONCE_COUNT;
        if (readForMergingBatchAddress != 0) {
            pageManager.freePages(readForMergingBatchAddress, npagesMerge);
            System.out.println("Clean up chunk, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", read page address: " + readForMergingBatchAddress);

            readForMergingBatchAddress = 0;
            readForMergingBatchBuffer = null;
        }

        if (readSelfForMergingBatchAddress != 0) {
            pageManager.freePages(readSelfForMergingBatchAddress, npagesMerge);
            System.out.println("Clean up chunk, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", read self page address: " + readSelfForMergingBatchAddress);

            readSelfForMergingBatchAddress = 0;
            readSelfForMergingBatchBuffer = null;
        }

        if (fds != null) {
            for (var fd : fds) {
                int r = libC.close(fd);
                if (r < 0) {
                    System.err.println("Close fd error: " + libC.strerror(r));
                }
            }
            System.out.println("Closed fds, length: " + fds.length);
        }
        if (readSegmentBufferAddresses != null) {
            for (var addr : readSegmentBufferAddresses) {
                pageManager.freePages(addr, npages0);
            }
            System.out.println("Freed read segment buffer addresses, length: " + readSegmentBufferAddresses.length);
        }
    }

    // begin with 0
    // -1 means not init
    int segmentIndex = -1;

    int targetFdIndex() {
        return segmentIndex / maxSegmentNumberPerFd;
    }

    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / maxSegmentNumberPerFd;
    }

    public boolean initIndex(int segmentIndex) {
        log.info("Chunk init w={}, s={}, b={}, i={}", workerId, slot, batchIndex, segmentIndex);
        this.segmentIndex = segmentIndex;
        return reuseSegments(true, 1, true);
    }

    private boolean reuseSegments(boolean isInit, int segmentCount, boolean updateFlag) {
        for (int i = 0; i < segmentCount; i++) {
            var targetIndex = segmentIndex + i;

            var segmentFlag = oneSlot.getSegmentMergeFlag(workerId, batchIndex, targetIndex);
            if (segmentFlag == null) {
                continue;
            }

            var flag = segmentFlag.flag();
            if (flag == SEGMENT_FLAG_INIT) {
                continue;
            }

            // init segment index already set flag to reuse
            if (!isInit && flag == SEGMENT_FLAG_REUSE) {
                continue;
            }

            if (flag == SEGMENT_FLAG_MERGED_AND_PERSISTED || flag == SEGMENT_FLAG_REUSE_AND_PERSISTED) {
                if (updateFlag) {
                    oneSlot.setSegmentMergeFlag(workerId, batchIndex, targetIndex, SEGMENT_FLAG_REUSE, workerId, 0L);
                }
                continue;
            }

            log.warn("Chunk segment index is not merged and persisted or reuse and persisted, can not write, w={}, s={}, b={}, i={}, flag={}",
                    workerId, slot, batchIndex, targetIndex, flag);
            return false;
        }
        return true;
    }

    // for seq generation
    final SnowFlake snowFlake;

    public record SegmentFlag(byte flag, byte workerId, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentFlag{" +
                    "flag=" + flag +
                    ", workerId=" + workerId +
                    ", segmentSeq=" + segmentSeq +
                    '}';
        }
    }

    private int lastPersistCvCount = 0;

    // use zstd compress, default level 3
    static final int COMPRESS_LEVEL = 3;

    private static final int LEFT_SEGMENT_COUNT_NO_USE = 100;
    // need refer Wal valueSizeTrigger
    // todo, use config value better
    static final int ONCE_PREPARE_SEGMENT_COUNT = 100;

    long threadIdProtected = -1;

    private static final ArrayList<Integer> EMPTY_LIST = new ArrayList<>();

    // return need merge segment index array
    public ArrayList<Integer> persist(ArrayList<Wal.V> list) {
        var currentThreadId = Thread.currentThread().threadId();
        if (threadIdProtected != -1 && threadIdProtected != currentThreadId) {
            throw new IllegalStateException("Thread id not match, w=" + workerId + ", s=" + slot + ", b=" + batchIndex +
                    ", t=" + currentThreadId + ", t2=" + threadIdProtected);
        }
        if (Debug.getInstance().perfSkipPersist) {
            return EMPTY_LIST;
        }

        moveIndexForPrepare();
        boolean canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT, false);
        if (!canWrite) {
            throw new SegmentOverflowException("Segment can not write, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", i=" + segmentIndex);
        }

        int[] nextNSegmentIndex = new int[ONCE_PREPARE_SEGMENT_COUNT];
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT; i++) {
            nextNSegmentIndex[i] = segmentIndex + i;
        }

        long beginT = System.nanoTime();
        boolean isNewAppendAfterBatch = true;

        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = segmentBatch.splitAndTight(list, nextNSegmentIndex, pvmList);
        if (segments == null) {
            return null;
        }

        ArrayList<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }
        oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, segments.size(),
                SEGMENT_FLAG_REUSE, workerId, segmentSeqListAll);

        if (segments.size() < BATCH_SEGMENT_COUNT_FOR_PWRITE) {
            for (var segment : segments) {
                writePageBuffer.clear();
                writePageBuffer.put(segment.tightBytesWithLength());

                ArrayList<Long> segmentSeqListSubBatch = new ArrayList<>();
                segmentSeqListSubBatch.add(segment.segmentSeq());

                boolean isNewAppend = writeSegments(writePageBuffer, 1, segmentSeqListSubBatch);
                isNewAppendAfterBatch = isNewAppend;

                // need set segment flag so that merge worker can merge
                oneSlot.setSegmentMergeFlag(workerId, batchIndex, segment.segmentIndex(),
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segment.segmentSeq());

                segmentIndex++;
            }
        } else {
            // batch write
            var batchCount = segments.size() / BATCH_SEGMENT_COUNT_FOR_PWRITE;
            var remainCount = segments.size() % BATCH_SEGMENT_COUNT_FOR_PWRITE;

            for (int i = 0; i < batchCount; i++) {
                writePageBufferB.clear();
                ArrayList<Long> segmentSeqListSubBatch = new ArrayList<>();
                for (int j = 0; j < BATCH_SEGMENT_COUNT_FOR_PWRITE; j++) {
                    var segment = segments.get(i * BATCH_SEGMENT_COUNT_FOR_PWRITE + j);
                    var tightBytesWithLength = segment.tightBytesWithLength();
                    writePageBufferB.put(tightBytesWithLength);

                    // padding to segment length
                    if (tightBytesWithLength.length < segmentLength) {
                        writePageBufferB.position(writePageBufferB.position() + segmentLength - tightBytesWithLength.length);
                    }

                    segmentSeqListSubBatch.add(segment.segmentSeq());
                }

                boolean isNewAppend = writeSegments(writePageBufferB, BATCH_SEGMENT_COUNT_FOR_PWRITE, segmentSeqListSubBatch);
                isNewAppendAfterBatch = isNewAppend;

                // need set segment flag so that merge worker can merge
                oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, BATCH_SEGMENT_COUNT_FOR_PWRITE,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqListSubBatch);

                segmentIndex += BATCH_SEGMENT_COUNT_FOR_PWRITE;
            }

            for (int i = 0; i < remainCount; i++) {
                var segment = segments.get(batchCount * BATCH_SEGMENT_COUNT_FOR_PWRITE + i);
                writePageBuffer.clear();
                writePageBuffer.put(segment.tightBytesWithLength());

                ArrayList<Long> segmentSeqListSubBatch = new ArrayList<>();
                segmentSeqListSubBatch.add(segment.segmentSeq());

                boolean isNewAppend = writeSegments(writePageBuffer, 1, segmentSeqListSubBatch);
                isNewAppendAfterBatch = isNewAppend;

                // need set segment flag so that merge worker can merge
                oneSlot.setSegmentMergeFlag(workerId, batchIndex, segment.segmentIndex(),
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segment.segmentSeq());

                segmentIndex++;
            }
        }
        long costT = System.nanoTime() - beginT;
        persistCostTotalTimeMicros += costT / 1000;

        persistCount++;
        persistSegmentCount += segments.size();
        persistCvCount += list.size();
        lastPersistCvCount = list.size();

        if (!Debug.getInstance().perfSkipPvmUpdate) {
            for (var segment : segments) {
                oneSlot.refreshReadPersistedSegmentCache(workerId, batchIndex, segment.segmentIndex(), segment.tightBytesWithLength());

                if (segment.segmentIndex() % 10000 == 0 && slot == 0) {
                    log.info("Segment cache!, w={}, s={}, b={}, i={}", workerId, slot, batchIndex, segment.segmentIndex());
                }
            }

            beginT = System.nanoTime();
            keyLoader.updatePvmListAfterWriteSegment(pvmList);
            costT = System.nanoTime() - beginT;
            updatePvmCostTotalTimeMicros += costT / 1000;
        }

        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();
        // top merge worker use a schedule to merge self
        if (isRequestWorker || isMergeWorker) {
            for (var segment : segments) {
                int toMergeSegmentIndex = needMergeSegmentIndex(isNewAppendAfterBatch, segment.segmentIndex());
                if (toMergeSegmentIndex != -1) {
                    needMergeSegmentIndexList.add(toMergeSegmentIndex);
                }
            }
        }

        oneSlot.setChunkWriteSegmentIndex(workerId, batchIndex, segmentIndex);
        return needMergeSegmentIndexList;
    }

    private void moveIndexForPrepare() {
        int leftSegmentCountThisFd = maxSegmentNumberPerFd - segmentIndex % maxSegmentNumberPerFd;
        if (leftSegmentCountThisFd < LEFT_SEGMENT_COUNT_NO_USE) {
            // begin with next fd
            segmentIndex += leftSegmentCountThisFd;
        }
        if (segmentIndex >= maxSegmentIndex) {
            segmentIndex = 0;
        }
    }

    void moveIndexNext(int segmentCount) {
        if (segmentCount > 1) {
            // already skip fd last 100 segments
            segmentIndex += segmentCount;
            if (segmentIndex == maxSegmentIndex) {
                segmentIndex = 0;
            }
            return;
        }

        if (segmentIndex == maxSegmentIndex) {
            log.warn("Chunk segment index reach max reuse, w={}, s={}, b={}, i={}", workerId, slot, batchIndex, segmentIndex);
            segmentIndex = 0;

            reuseLoopCount++;
        } else {
            segmentIndex++;

            if (segmentIndex == halfSegmentIndex) {
                log.warn("Chunk segment index reach half max reuse, w={}, s={}, b={}, i={}", workerId, slot, batchIndex, segmentIndex);
            }
        }
    }

    private int needMergeSegmentIndex(boolean isNewAppend, int targetIndex) {
        int segmentIndexToMerge = -1;
        if (targetIndex >= halfSegmentIndex) {
            // begins with 0
            // ends with 2^18 - 1
            segmentIndexToMerge = targetIndex - halfSegmentIndex;
        } else {
            if (!isNewAppend) {
                // begins with 2^18
                // ends with 2^19 - 1
                segmentIndexToMerge = targetIndex + halfSegmentIndex + 1 + 1;
            }
        }
        return segmentIndexToMerge;
    }

    private long pwriteCostNanos;
    private long pwriteCount;

    private boolean writeSegments(ByteBuffer writeBuffer, int segmentCount, ArrayList<Long> segmentSeqList) {
        int fdIndex = targetFdIndex();
        int fd = fds[fdIndex];

        writeBuffer.rewind();
        writeBuffer.mark();

        long offset = (long) segmentIndex * segmentLength;
        var fdOffset = (int) (offset % maxFdLength);

        long beginT = System.nanoTime();
        int n = libC.pwrite(fd, writeBuffer, writeBuffer.capacity(), fdOffset);
        long costT = System.nanoTime() - beginT;
        if (n != writeBuffer.capacity()) {
            throw new RuntimeException("Write chunk error, fd: " + fd + ", segment index: " + segmentIndex + ", last error: " +
                    libC.strerror(n));
        }

        pwriteCostNanos += costT;
        pwriteCount++;

        boolean isNewAppend = false;
        int afterThisBatchOffset = (segmentIndex + segmentCount) * segmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
            isNewAppend = true;
        }

        if (masterUpdateCallback != null) {
            writeBuffer.reset();
            var bytes = new byte[writeBuffer.remaining()];
            writeBuffer.get(bytes);
            masterUpdateCallback.onSegmentWrite(workerId, batchIndex, slot, segmentLength,
                    segmentIndex, segmentCount, segmentSeqList, bytes, n);
        }
        return isNewAppend;
    }

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();

        final String prefix = "chunk-w-" + workerId + "-s-" + slot + "-b-" + batchIndex + " ";

        list.add(new StatKV(prefix + "current segment index", segmentIndex));
        list.add(new StatKV(prefix + "reuse loop count", reuseLoopCount));
        list.add(new StatKV(prefix + "persist count", persistCount));
        list.add(new StatKV(prefix + "persist segment count", persistSegmentCount));
        list.add(new StatKV(prefix + "persist cv count", persistCvCount));

        long persistByteLength = segmentLength * persistSegmentCount;
        list.add(new StatKV(prefix + "persist bytes", persistByteLength));
        list.add(new StatKV(prefix + "last persist cv count", lastPersistCvCount));

        list.add(new StatKV(prefix + "persist cost total micros", persistCostTotalTimeMicros));
        double writeCostAvgMicros = (double) persistCostTotalTimeMicros / persistCount;
        list.add(new StatKV(prefix + "persist cost avg micros", writeCostAvgMicros));

        list.add(new StatKV(prefix + "pwrite count", pwriteCount));
        if (pwriteCount > 0) {
            double pwriteCostAvgMicros = (double) pwriteCostNanos / pwriteCount / 1000;
            list.add(new StatKV(prefix + "pwrite cost avg micros", pwriteCostAvgMicros));
        }

        list.add(new StatKV(prefix + "update pvm cost total micros", updatePvmCostTotalTimeMicros));
        double updatePvmCostAvgMicros = (double) updatePvmCostTotalTimeMicros / persistCount;
        list.add(new StatKV(prefix + "update pvm cost avg micros", updatePvmCostAvgMicros));

        var perfTestReadSegmentNoCache = Debug.getInstance().perfTestReadSegmentNoCache;
        if (perfTestReadSegmentNoCache) {
            if (preadCount > 0) {
                list.add(new StatKV(prefix + "pread count", preadCount));
                double preadCostAvgMicros = (double) preadCostNanos / preadCount / 1000;
                list.add(new StatKV(prefix + "pread cost avg micros", preadCostAvgMicros));
            }
        }

        list.add(StatKV.split);
        return list;
    }
}
