package redis.persist;

import io.prometheus.client.Counter;
import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.Debug;
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
    private final int segmentNumberPerFd;
    private final byte fdPerChunk;
    final int maxSegmentIndex;
    final int halfSegmentNumber;

    // seq long + cv number int + crc int
    public static final int SEGMENT_HEADER_LENGTH = 8 + 4 + 4;
    public static final byte SEGMENT_FLAG_INIT = 100;
    public static final byte SEGMENT_FLAG_NEW = 0;
    public static final byte SEGMENT_FLAG_REUSE_AND_PERSISTED = 10;
    public static final byte SEGMENT_FLAG_MERGING = 1;
    public static final byte SEGMENT_FLAG_MERGED = -1;
    public static final byte SEGMENT_FLAG_MERGED_AND_PERSISTED = -10;
    public static final byte SEGMENT_FLAG_REUSE = -100;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final byte slot;
    private final String slotStr;
    private final File slotDir;

    // for better latency, segment length = 4096 decompress performance is better
    final int segmentLength;

    private static final Counter persistCounter = Counter.build().name("chunk_persist_count").
            help("chunk persist count").
            labelNames("slot")
            .register();

    private static final Counter persistCvCounter = Counter.build().name("chunk_persist_cv_count").
            help("chunk persist cv count").
            labelNames("slot")
            .register();

    private static final Counter updatePvmBatchCostTimeTotalUs = Counter.build().name("pvm_update_cost_time_total_us").
            help("key loader pvm update cost time total us").
            labelNames("slot")
            .register();

    private final OneSlot oneSlot;
    private final KeyLoader keyLoader;
    private final MasterUpdateCallback masterUpdateCallback;
    private final SegmentBatch segmentBatch;

    int[] fdLengths;
    FdReadWrite[] fdReadWriteArray;

    public Chunk(byte slot, File slotDir, OneSlot oneSlot,
                 SnowFlake snowFlake, KeyLoader keyLoader, MasterUpdateCallback masterUpdateCallback) {
        var c = ConfForSlot.global.confChunk;
        this.segmentNumberPerFd = c.segmentNumberPerFd;
        this.fdPerChunk = c.fdPerChunk;

        int maxSegmentNumber = c.maxSegmentNumber();
        this.maxSegmentIndex = maxSegmentNumber - 1;
        this.halfSegmentNumber = maxSegmentNumber / 2;

        log.info("Chunk init slot={}, segment number per fd={}, fd per chunk={}, max segment index={}, half segment number={}",
                slot, segmentNumberPerFd, fdPerChunk, maxSegmentIndex, halfSegmentNumber);

        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = slotDir;

        this.segmentLength = c.segmentLength;

        this.oneSlot = oneSlot;
        this.keyLoader = keyLoader;
        this.masterUpdateCallback = masterUpdateCallback;
        this.segmentBatch = new SegmentBatch(slot, snowFlake);
    }

    void initFds(LibC libC) throws IOException {
        this.fdLengths = new int[fdPerChunk];
        this.fdReadWriteArray = new FdReadWrite[fdPerChunk];
        for (int i = 0; i < fdPerChunk; i++) {
            // prometheus metric labels use _ instead of -
            var name = "chunk_data_index_" + i;
            var file = new File(slotDir, "chunk-data-" + i);
            fdLengths[i] = (int) file.length();

            var fdReadWrite = new FdReadWrite(name, libC, file);
            fdReadWrite.initByteBuffers(true);

            this.fdReadWriteArray[i] = fdReadWrite;
        }
    }

    void cleanUp() {
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
        return fdReadWrite.readSegment(segmentIndexTargetFd, true);
    }

    // begin with 0
    // -1 means not init
    int segmentIndex = -1;

    int currentSegmentIndex() {
        return segmentIndex;
    }

    int targetFdIndex() {
        return segmentIndex / segmentNumberPerFd;
    }

    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / segmentNumberPerFd;
    }

    int targetSegmentIndexTargetFd() {
        return segmentIndex % segmentNumberPerFd;
    }

    int targetSegmentIndexTargetFd(int targetSegmentIndex) {
        return targetSegmentIndex % segmentNumberPerFd;
    }

    public boolean initSegmentIndexWhenFirstStart(int segmentIndex) {
        log.info("Chunk init s={}, i={}", slot, segmentIndex);
        if (segmentIndex > maxSegmentIndex) {
            segmentIndex = 0;
        }
        this.segmentIndex = segmentIndex;
        return reuseSegments(true, 1, true);
    }

    private boolean reuseSegments(boolean isFirstStart, int segmentCount, boolean updateAsReuseFlag) {
        for (int i = 0; i < segmentCount; i++) {
            var targetIndex = segmentIndex + i;

            var segmentFlag = oneSlot.getSegmentMergeFlag(targetIndex);
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
                if (updateAsReuseFlag) {
                    oneSlot.setSegmentMergeFlag(targetIndex, SEGMENT_FLAG_REUSE, 0L, segmentFlag.walGroupIndex);
                }
                continue;
            }

            // only left SEGMENT_FLAG_NEW, SEGMENT_FLAG_MERGING, SEGMENT_FLAG_MERGED, can not reuse
            log.warn("Chunk segment index is not merged and persisted or reuse and persisted, can not write, s={}, i={}, flag={}",
                    slot, targetIndex, flag);
            return false;
        }
        return true;
    }

    public record SegmentFlag(byte flag, long segmentSeq, int walGroupIndex) {
        @Override
        public String toString() {
            return "SegmentFlag{" +
                    "flag=" + flag +
                    ", segmentSeq=" + segmentSeq +
                    ", walGroupIndex=" + walGroupIndex +
                    '}';
        }
    }

    // need refer Wal valueSizeTrigger
    // 32 not tight segments is enough for 1000 Wal.V persist ?, need check, todo
    // one wal group charges 64 key buckets, 64 * 4KB = 256KB
    static final int ONCE_PREPARE_SEGMENT_COUNT = 32;

    int mergedSegmentIndexEndLastTime = NO_NEED_MERGE_SEGMENT_INDEX;

    void checkMergedSegmentIndexEndLastTimeValidAfterServerStart() {
        if (mergedSegmentIndexEndLastTime != NO_NEED_MERGE_SEGMENT_INDEX) {
            if (mergedSegmentIndexEndLastTime < 0 || mergedSegmentIndexEndLastTime >= maxSegmentIndex) {
                throw new IllegalStateException("Merged segment index end last time out of bound, s=" + slot + ", i=" + mergedSegmentIndexEndLastTime);
            }
        }
    }

    // return need merge segment index array
    public ArrayList<Integer> persist(int walGroupIndex, ArrayList<Wal.V> list, boolean isMerge) {
        logMergeCount++;
        var doLog = (Debug.getInstance().logMerge && logMergeCount % 1000 == 0);

        moveIndexForPrepare();
        boolean canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT, false);
        if (!canWrite) {
            throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex);
        }

        int[] nextNSegmentIndex = new int[ONCE_PREPARE_SEGMENT_COUNT];
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT; i++) {
            nextNSegmentIndex[i] = segmentIndex + i;
        }

        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = segmentBatch.splitAndTight(list, nextNSegmentIndex, pvmList);

        List<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }
        oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                SEGMENT_FLAG_REUSE, segmentSeqListAll, walGroupIndex);

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

            oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                    isNewAppendAfterBatch ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segmentSeqListAll, walGroupIndex);
        } else {
            if (segments.size() < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    segmentSeqListSubBatch.add(segment.segmentSeq());

                    boolean isNewAppend = writeSegments(segment.tightBytesWithLength(), 1);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(segment.segmentIndex(),
                            isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);
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

                    boolean isNewAppend = writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlagBatch(segmentIndex, BATCH_ONCE_SEGMENT_COUNT_PWRITE,
                            isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segmentSeqListSubBatch, walGroupIndex);

                    moveSegmentIndexNext(BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                }

                for (int i = 0; i < remainCount; i++) {
                    var segment = segments.get(batchCount * BATCH_ONCE_SEGMENT_COUNT_PWRITE + i);

                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    segmentSeqListSubBatch.add(segment.segmentSeq());

                    boolean isNewAppend = writeSegments(segment.tightBytesWithLength(), 1);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(segment.segmentIndex(),
                            isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);
                }
            }
        }

        // stats
        persistCounter.labels(slotStr).inc();
        persistCvCounter.labels(slotStr).inc(list.size());

        var beginT = System.nanoTime();
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, isMerge);
        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        updatePvmBatchCostTimeTotalUs.labels(slotStr).inc(costT);

        // update meta, segment index for next time
        oneSlot.setChunkWriteSegmentIndex(segmentIndex);

        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();
        if (isMerge) {
            // return empty list
            return needMergeSegmentIndexList;
        }

        for (var segment : segments) {
            var toMergeSegmentIndex = needMergeSegmentIndex(isNewAppendAfterBatch, segment.segmentIndex());
            if (toMergeSegmentIndex != NO_NEED_MERGE_SEGMENT_INDEX) {
                needMergeSegmentIndexList.add(toMergeSegmentIndex);
            }
        }

        if (needMergeSegmentIndexList.isEmpty()) {
            return needMergeSegmentIndexList;
        }

        needMergeSegmentIndexList.sort(Integer::compareTo);

        // recycle, need spit to two part
        if (needMergeSegmentIndexList.getLast() - needMergeSegmentIndexList.getFirst() > halfSegmentNumber) {
            assert needMergeSegmentIndexList.contains(0);
            assert mergedSegmentIndexEndLastTime != NO_NEED_MERGE_SEGMENT_INDEX;

            var onePart = new ArrayList<Integer>();
            var anotherPart = new ArrayList<Integer>();
            for (var segmentIndex : needMergeSegmentIndexList) {
                if (segmentIndex < oneSlot.chunk.halfSegmentNumber) {
                    onePart.add(segmentIndex);
                } else {
                    anotherPart.add(segmentIndex);
                }
            }
            log.warn("Recycle merge chunk, s={}, one part need merge segment index list ={}, another part need merge segment index list={}",
                    slot, onePart, anotherPart);


            // prepend from merged segment index end last time
            var firstNeedMergeSegmentIndex = anotherPart.getFirst();
            assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
            for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                anotherPart.add(i);
            }
            anotherPart.sort(Integer::compareTo);

            checkNeedMergeSegmentIndexListContinuous(onePart);
            checkNeedMergeSegmentIndexListContinuous(anotherPart);

            mergedSegmentIndexEndLastTime = onePart.getLast();
        } else {
            var firstNeedMergeSegmentIndex = needMergeSegmentIndexList.getFirst();
            if (mergedSegmentIndexEndLastTime == NO_NEED_MERGE_SEGMENT_INDEX) {
                if (firstNeedMergeSegmentIndex != 0) {
                    throw new IllegalStateException("First need merge segment index not 0, s=" + slot + ", i=" + firstNeedMergeSegmentIndex);
                }
            } else {
                // prepend from merged segment index end last time
                assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
                for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                    needMergeSegmentIndexList.add(i);
                }
                needMergeSegmentIndexList.sort(Integer::compareTo);
            }

            checkNeedMergeSegmentIndexListContinuous(needMergeSegmentIndexList);
            mergedSegmentIndexEndLastTime = needMergeSegmentIndexList.getLast();
        }

        if (doLog) {
            log.info("Chunk persist need merge segment index list, s={}, i={}, list={}", slot, segmentIndex, needMergeSegmentIndexList);
        }

        return needMergeSegmentIndexList;
    }

    private void checkNeedMergeSegmentIndexListContinuous(ArrayList<Integer> list) {
        if (list.size() == 1) {
            return;
        }

        final int maxSize = ONCE_PREPARE_SEGMENT_COUNT * 2 + ChunkMergeWorker.MERGED_SEGMENT_SET_SIZE_THRESHOLD;

        list.sort(Integer::compareTo);
        if (list.getLast() - list.getFirst() != list.size() - 1) {
            throw new IllegalStateException("Need merge segment index not continuous, s=" + slot +
                    ", first need merge segment index=" + list.getFirst() +
                    ", last need merge segment index=" + list.getLast() +
                    ", last time merged segment index =" + mergedSegmentIndexEndLastTime +
                    ", list size=" + list.size());
        }

        if (list.size() > maxSize) {
            throw new IllegalStateException("Need merge segment index list size too large, s=" + slot +
                    ", first need merge segment index=" + list.getFirst() +
                    ", last need merge segment index=" + list.getLast() +
                    ", last time merged segment index =" + mergedSegmentIndexEndLastTime +
                    ", list size=" + list.size());
        }
    }

    private long logMergeCount = 0;

    private void moveIndexForPrepare() {
        int leftSegmentCountThisFd = segmentNumberPerFd - segmentIndex % segmentNumberPerFd;
        if (leftSegmentCountThisFd < ONCE_PREPARE_SEGMENT_COUNT) {
            // begin with next fd
            segmentIndex += leftSegmentCountThisFd;
        }
        if (segmentIndex >= maxSegmentIndex) {
            segmentIndex = 0;
        }
    }

    void moveSegmentIndexNext(int segmentCount) {
        if (segmentCount > 1) {
            var newSegmentIndex = segmentIndex + segmentCount;
            if (newSegmentIndex == maxSegmentIndex) {
                newSegmentIndex = 0;
            } else if (newSegmentIndex > maxSegmentIndex) {
                // already skip fd last segments for prepare pwrite batch, never reach here
                throw new SegmentOverflowException("Segment index overflow, s=" + slot + ", i=" + segmentIndex +
                        ", c=" + segmentCount + ", max=" + maxSegmentIndex);
            }
            segmentIndex = newSegmentIndex;
            return;
        }

        if (segmentIndex == maxSegmentIndex) {
            log.warn("Chunk segment index reach max reuse, s={}, i={}", slot, segmentIndex);
            segmentIndex = 0;
        } else {
            segmentIndex++;

            if (segmentIndex == halfSegmentNumber) {
                log.warn("Chunk segment index reach half of max, start reuse from beginning, s={}, i={}", slot, segmentIndex);
            }
        }
    }

    private static final int NO_NEED_MERGE_SEGMENT_INDEX = -1;

    int needMergeSegmentIndex(boolean isNewAppend, int targetIndex) {
        int segmentIndexToMerge = NO_NEED_MERGE_SEGMENT_INDEX;
        if (targetIndex >= halfSegmentNumber) {
            // begins with 0
            // ends with 2^18 - 1
            segmentIndexToMerge = targetIndex - halfSegmentNumber;
        } else {
            if (!isNewAppend) {
                segmentIndexToMerge = targetIndex + halfSegmentNumber;
            }
        }
        return segmentIndexToMerge;
    }

    public boolean writeSegmentsFromMasterExists(byte[] bytes, int segmentIndex, int segmentCount, List<Long> segmentSeqList, int walGroupIndex, int capacity) {
        if (ConfForSlot.global.pureMemory) {
            if (capacity != bytes.length) {
                throw new IllegalArgumentException("Write buffer capacity not match, expect: " + capacity + ", actual: " + bytes.length);
            }

            var fdIndex = targetFdIndex(segmentIndex);
            var segmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

            var fdReadWrite = fdReadWriteArray[fdIndex];

            var isNewAppend = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            if (segmentCount == 1) {
                fdReadWrite.writeSegment(segmentIndexTargetFd, bytes, false);

                oneSlot.setSegmentMergeFlag(segmentIndex,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segmentSeqList.getFirst(), walGroupIndex);
            } else {
                for (int i = 0; i < segmentCount; i++) {
                    var oneSegmentBytes = new byte[segmentLength];
                    System.arraycopy(bytes, i * segmentLength, oneSegmentBytes, 0, segmentLength);

                    fdReadWrite.writeSegment(segmentIndexTargetFd + i, oneSegmentBytes, false);
                }

                oneSlot.setSegmentMergeFlagBatch(segmentIndex, segmentCount,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segmentSeqList, walGroupIndex);
            }

            return isNewAppend;
        } else {
            this.segmentIndex = segmentIndex;
            boolean isNewAppend = writeSegments(bytes, segmentCount);

            if (segmentCount == 1) {
                oneSlot.setSegmentMergeFlag(segmentIndex,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segmentSeqList.getFirst(), walGroupIndex);
            } else {
                oneSlot.setSegmentMergeFlagBatch(segmentIndex, segmentCount,
                        isNewAppend ? SEGMENT_FLAG_NEW : SEGMENT_FLAG_REUSE_AND_PERSISTED, segmentSeqList, walGroupIndex);
            }

            return isNewAppend;
        }
    }

    private boolean writeSegments(byte[] bytes, int segmentCount) {
        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        if (segmentCount == 1) {
            fdReadWrite.writeSegment(segmentIndexTargetFd, bytes, false);
        } else if (segmentCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            fdReadWrite.writeSegmentBatch(segmentIndexTargetFd, bytes, false);
        } else if (segmentCount == ONCE_SEGMENT_COUNT) {
            fdReadWrite.writeSegmentForRepl(segmentIndexTargetFd, bytes, 0);
        } else {
            throw new IllegalArgumentException("Write segment count not support: " + segmentCount);
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
