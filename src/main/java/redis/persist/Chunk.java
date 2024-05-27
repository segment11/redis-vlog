package redis.persist;

import io.prometheus.client.Counter;
import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.SnowFlake;
import redis.repl.MasterUpdateCallback;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static redis.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE;
import static redis.repl.content.ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT;

public class Chunk {
    private final int maxSegmentNumberPerFd;
    private final byte maxFdPerChunk;
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

    final byte workerId;
    final byte batchIndex;
    final byte slot;

    private final String workerIdStr;
    private final String batchIndexStr;
    private final String slotStr;

    // for better latency, segment length = 4096 decompress performance is better
    final int segmentLength;
    // for seq generation
    final SnowFlake snowFlake;
    private final File slotDir;

    private static final Counter persistCounter = Counter.build().name("persist_count").
            help("chunk persist count").
            labelNames("worker_id", "batch_index", "slot")
            .register();

    private static final Counter persistCvCounter = Counter.build().name("persist_cv_count").
            help("chunk persist cv count").
            labelNames("worker_id", "batch_index", "slot")
            .register();

    private static final Counter updatePvmBatchCostMillisCounter = Counter.build().name("pvm_update_ms").
            help("key loader update pvm batch cost in millis").
            labelNames("worker_id", "batch_index", "slot")
            .register();

    private final OneSlot oneSlot;
    private final KeyLoader keyLoader;
    private final MasterUpdateCallback masterUpdateCallback;
    private final SegmentBatch segmentBatch;

    private int[] fdLengths;
    private FdReadWrite[] fdReadWriteArray;

    public Chunk(byte workerId, byte slot, byte batchIndex, byte requestWorkers,
                 SnowFlake snowFlake, File slotDir, OneSlot oneSlot, KeyLoader keyLoader, MasterUpdateCallback masterUpdateCallback) {
        this.maxSegmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
        this.maxFdPerChunk = ConfForSlot.global.confChunk.fdPerChunk;

        int maxSegmentNumber = maxSegmentNumberPerFd * maxFdPerChunk;
        this.maxSegmentIndex = maxSegmentNumber - 1;
        this.halfSegmentIndex = maxSegmentNumber / 2;

        this.workerId = workerId;
        this.batchIndex = batchIndex;
        this.slot = slot;

        this.workerIdStr = String.valueOf(workerId);
        this.batchIndexStr = String.valueOf(batchIndex);
        this.slotStr = String.valueOf(slot);

        this.segmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.snowFlake = snowFlake;
        this.slotDir = slotDir;

        this.oneSlot = oneSlot;
        this.keyLoader = keyLoader;
        this.masterUpdateCallback = masterUpdateCallback;
        this.segmentBatch = new SegmentBatch(workerId, slot, batchIndex, snowFlake);
    }

    public void initFds(LibC libC) throws IOException {
        var chunkDir = new File(slotDir, "chunk-w-" + workerId + "-b-" + batchIndex);
        if (!chunkDir.exists()) {
            var isOk = chunkDir.mkdirs();
            if (!isOk) {
                throw new IOException("Create chunk dir error: " + chunkDir.getAbsolutePath());
            }
        }

        this.fdLengths = new int[maxFdPerChunk];
        this.fdReadWriteArray = new FdReadWrite[maxFdPerChunk];
        for (int i = 0; i < maxFdPerChunk; i++) {
            // prometheus metric labels use _ instead of -
            var name = "chunk_data_w_" + workerId + "_b_" + batchIndex + "_index_" + i;
            var file = new File(chunkDir, "chunk-data-" + i);
            fdLengths[i] = (int) file.length();

            var fdReadWrite = new FdReadWrite(name, libC, file);
            fdReadWrite.initByteBuffers(true);
            fdReadWrite.initEventloop();

            this.fdReadWriteArray[i] = fdReadWrite;
        }
    }

    public void cleanUp() {
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                fdReadWrite.cleanUp();
            }
        }
    }

    byte[] preadForMerge(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentForMerge(segmentIndexTargetFd);
    }

    byte[] preadForRepl(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentForRepl(segmentIndexTargetFd);
    }

    byte[] preadSegmentTightBytesWithLength(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];

        var segmentBytes = fdReadWrite.readSegment(segmentIndexTargetFd, true);
        return segmentBytes;

//        var buffer = ByteBuffer.wrap(segmentBytes);
//
//        int tightBytesLength = buffer.getInt();
//        var tightBytesWithLength = new byte[4 + tightBytesLength];
//        buffer.position(0).get(tightBytesWithLength);
//        return tightBytesWithLength;
    }

    // begin with 0
    // -1 means not init
    private int segmentIndex = -1;

    int currentSegmentIndex() {
        return segmentIndex;
    }

    int targetFdIndex() {
        return segmentIndex / maxSegmentNumberPerFd;
    }

    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / maxSegmentNumberPerFd;
    }

    int targetSegmentIndexTargetFd() {
        return segmentIndex % maxSegmentNumberPerFd;
    }

    int targetSegmentIndexTargetFd(int targetSegmentIndex) {
        return targetSegmentIndex % maxSegmentNumberPerFd;
    }

    public boolean initSegmentIndexWhenFirstStart(int segmentIndex) {
        log.info("Chunk init w={}, s={}, b={}, i={}", workerId, slot, batchIndex, segmentIndex);
        this.segmentIndex = segmentIndex;
        return reuseSegments(true, 1, true);
    }

    private boolean reuseSegments(boolean isFirstStart, int segmentCount, boolean updateFlag) {
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
            if (!isFirstStart && flag == SEGMENT_FLAG_REUSE) {
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

    // need refer Wal valueSizeTrigger
    // 32 segments is enough for 1000 Wal.V persist
    static final int ONCE_PREPARE_SEGMENT_COUNT = 32;

    long threadIdProtectedWhenWrite = -1;

    // return need merge segment index array
    public ArrayList<Integer> persist(int walGroupIndex, ArrayList<Wal.V> list) {
        checkCurrentThread();

        moveIndexForPrepare();
        boolean canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT, false);
        if (!canWrite) {
            throw new SegmentOverflowException("Segment can not write, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", i=" + segmentIndex);
        }

        int[] nextNSegmentIndex = new int[ONCE_PREPARE_SEGMENT_COUNT];
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT; i++) {
            nextNSegmentIndex[i] = segmentIndex + i;
        }

        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = segmentBatch.splitAndTight(list, nextNSegmentIndex, pvmList);
        if (segments == null) {
            return null;
        }

        List<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }
        oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, segments.size(),
                SEGMENT_FLAG_REUSE, workerId, segmentSeqListAll);

        boolean isNewAppendAfterBatch = true;

        var firstSegmentIndex = segments.getFirst().segmentIndex();
        var fdIndex = targetFdIndex(firstSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(firstSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];

        if (ConfForSlot.global.pureMemory) {
            isNewAppendAfterBatch = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            for (var segment : segments) {
                byte[] bytes = segment.tightBytesWithLength();
                fdReadWrite.writeSegment(targetFdIndex(segment.segmentIndex()), bytes, false);

                if (masterUpdateCallback != null) {
                    List<Long> segmentSeqList = new ArrayList<>();
                    segmentSeqList.add(segment.segmentSeq());
                }
            }
            segmentIndex += segments.size();

            oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, segments.size(),
                    isNewAppendAfterBatch ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqListAll);
        } else {
            if (segments.size() < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    segmentSeqListSubBatch.add(segment.segmentSeq());

                    boolean isNewAppend = writeSegments(segment.tightBytesWithLength(), 1, segmentSeqListSubBatch);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(workerId, batchIndex, segment.segmentIndex(),
                            isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segment.segmentSeq());

                    moveIndexNext(1);
                }
            } else {
                // batch write
                var batchCount = segments.size() / BATCH_ONCE_SEGMENT_COUNT_PWRITE;
                var remainCount = segments.size() % BATCH_ONCE_SEGMENT_COUNT_PWRITE;

                var tmpBatchBytes = new byte[segmentLength * BATCH_ONCE_SEGMENT_COUNT_PWRITE];
                var buffer = ByteBuffer.wrap(tmpBatchBytes);

                for (int i = 0; i < batchCount; i++) {
                    buffer.clear();
                    if (i > 0) {
                        Arrays.fill(tmpBatchBytes, (byte) 0);
                    }

                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    for (int j = 0; j < BATCH_ONCE_SEGMENT_COUNT_PWRITE; j++) {
                        var segment = segments.get(i * BATCH_ONCE_SEGMENT_COUNT_PWRITE + j);
                        var tightBytesWithLength = segment.tightBytesWithLength();
                        buffer.put(tightBytesWithLength);

                        // padding to segment length
                        if (tightBytesWithLength.length < segmentLength) {
                            buffer.position(buffer.position() + segmentLength - tightBytesWithLength.length);
                        }

                        segmentSeqListSubBatch.add(segment.segmentSeq());
                    }

                    boolean isNewAppend = writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_PWRITE, segmentSeqListSubBatch);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, BATCH_ONCE_SEGMENT_COUNT_PWRITE,
                            isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqListSubBatch);

                    moveIndexNext(BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                }

                for (int i = 0; i < remainCount; i++) {
                    var segment = segments.get(batchCount * BATCH_ONCE_SEGMENT_COUNT_PWRITE + i);

                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    segmentSeqListSubBatch.add(segment.segmentSeq());

                    boolean isNewAppend = writeSegments(segment.tightBytesWithLength(), 1, segmentSeqListSubBatch);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(workerId, batchIndex, segment.segmentIndex(),
                            isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segment.segmentSeq());

                    moveIndexNext(1);
                }
            }

            persistCounter.labels(workerIdStr, batchIndexStr, slotStr).inc();
            persistCvCounter.labels(workerIdStr, batchIndexStr, slotStr).inc(list.size());
        }

        var beginT = System.currentTimeMillis();
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList);
        var costT = System.currentTimeMillis() - beginT;
        updatePvmBatchCostMillisCounter.labels(workerIdStr, batchIndexStr, slotStr).inc(costT);

        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();
        for (var segment : segments) {
            int toMergeSegmentIndex = needMergeSegmentIndex(isNewAppendAfterBatch, segment.segmentIndex());
            if (toMergeSegmentIndex != -1) {
                needMergeSegmentIndexList.add(toMergeSegmentIndex);
            }
        }

        oneSlot.setChunkWriteSegmentIndex(workerId, batchIndex, segmentIndex);
        return needMergeSegmentIndexList;
    }

    private void checkCurrentThread() {
        var currentThreadId = Thread.currentThread().threadId();
        if (threadIdProtectedWhenWrite != -1 && threadIdProtectedWhenWrite != currentThreadId) {
            throw new IllegalStateException("Thread id not match, w=" + workerId + ", s=" + slot + ", b=" + batchIndex +
                    ", t=" + currentThreadId + ", t2=" + threadIdProtectedWhenWrite);
        }
    }

    private void moveIndexForPrepare() {
        int leftSegmentCountThisFd = maxSegmentNumberPerFd - segmentIndex % maxSegmentNumberPerFd;
        if (leftSegmentCountThisFd < ONCE_PREPARE_SEGMENT_COUNT) {
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

    public boolean writeSegmentsFromMasterNewly(byte[] bytes, int segmentIndex, int segmentCount, List<Long> segmentSeqList, int capacity) {
        if (ConfForSlot.global.pureMemory) {
            if (capacity != bytes.length) {
                throw new RuntimeException("Write buffer capacity not match, expect: " + capacity + ", actual: " + bytes.length);
            }

            var fdIndex = targetFdIndex(segmentIndex);
            var segmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

            var fdReadWrite = fdReadWriteArray[fdIndex];

            var isNewAppend = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            if (segmentCount == 1) {
                fdReadWrite.writeSegment(segmentIndexTargetFd, bytes, false);

                oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqList.getFirst());
            } else {
                for (int i = 0; i < segmentCount; i++) {
                    // trim 0, todo
                    var oneSegmentBytes = new byte[segmentLength];
                    System.arraycopy(bytes, i * segmentLength, oneSegmentBytes, 0, segmentLength);

                    fdReadWrite.writeSegment(segmentIndexTargetFd + i, oneSegmentBytes, false);
                }

                oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, segmentCount,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqList);
            }

            return isNewAppend;
        } else {
            this.segmentIndex = segmentIndex;
            boolean isNewAppend = writeSegments(bytes, segmentCount, segmentSeqList);

            if (segmentCount == 1) {
                oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqList.getFirst());
            } else {
                oneSlot.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, segmentCount,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, workerId, segmentSeqList);
            }

            return isNewAppend;
        }
    }

    private boolean writeSegments(byte[] bytes, int segmentCount, List<Long> segmentSeqList) {
        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        if (segmentCount == 1) {
            fdReadWrite.writeSegment(segmentIndexTargetFd, bytes, true);
        } else if (segmentCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            fdReadWrite.writeSegmentBatch(segmentIndex, bytes, true);
        } else if (segmentCount == ONCE_SEGMENT_COUNT) {
            fdReadWrite.writeSegmentForRepl(segmentIndex, bytes, 0);
        } else {
            throw new RuntimeException("Write segment count not support: " + segmentCount);
        }

        boolean isNewAppend = false;
        int afterThisBatchOffset = (segmentIndex + segmentCount) * segmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
            isNewAppend = true;
        }

        return isNewAppend;
    }
}
