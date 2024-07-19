package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.Debug;
import redis.SnowFlake;
import redis.metric.SimpleGauge;
import redis.repl.MasterUpdateCallback;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static redis.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE;
import static redis.repl.content.ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT;

public class Chunk {
    private final int segmentNumberPerFd;
    private final byte fdPerChunk;
    final int maxSegmentIndex;
    final int halfSegmentNumber;

    // seq long + cv number int + crc int
    public static final int SEGMENT_HEADER_LENGTH = 8 + 4 + 4;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final byte slot;
    private final String slotStr;
    private final File slotDir;

    // for better latency, segment length = 4096 decompress performance is better
    final int chunkSegmentLength;

    final static SimpleGauge chunkPersistGauge = new SimpleGauge("chunk_persist", "chunk persist segments",
            "slot");

    static {
        chunkPersistGauge.register();
    }

    private long persistCountTotal;
    private long persistCvCountTotal;
    private long updatePvmBatchCostTimeTotalUsTotal;

    final OneSlot oneSlot;
    private final KeyLoader keyLoader;
    private final MasterUpdateCallback masterUpdateCallback;
    private final SegmentBatch segmentBatch;

    int[] fdLengths;
    FdReadWrite[] fdReadWriteArray;

    public Chunk(byte slot, File slotDir, OneSlot oneSlot,
                 SnowFlake snowFlake, KeyLoader keyLoader, MasterUpdateCallback masterUpdateCallback) {
        var confChunk = ConfForSlot.global.confChunk;
        this.segmentNumberPerFd = confChunk.segmentNumberPerFd;
        this.fdPerChunk = confChunk.fdPerChunk;

        int maxSegmentNumber = confChunk.maxSegmentNumber();
        this.maxSegmentIndex = maxSegmentNumber - 1;
        this.halfSegmentNumber = maxSegmentNumber / 2;

        log.info("Chunk init slot={}, segment number per fd={}, fd per chunk={}, max segment index={}, half segment number={}",
                slot, segmentNumberPerFd, fdPerChunk, maxSegmentIndex, halfSegmentNumber);

        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = slotDir;

        this.chunkSegmentLength = confChunk.segmentLength;

        this.oneSlot = oneSlot;
        this.keyLoader = keyLoader;
        this.masterUpdateCallback = masterUpdateCallback;
        this.segmentBatch = new SegmentBatch(slot, snowFlake);

        this.initMetricsCollect();
    }

    private void initMetricsCollect() {
        chunkPersistGauge.addRawGetter(() -> {
            var labelValues = List.of(slotStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("persist_count_total", new SimpleGauge.ValueWithLabelValues((double) persistCountTotal, labelValues));
            map.put("persist_cv_count_total", new SimpleGauge.ValueWithLabelValues((double) persistCvCountTotal, labelValues));

            map.put("update_pvm_batch_cost_time_total_us_total", new SimpleGauge.ValueWithLabelValues((double) updatePvmBatchCostTimeTotalUsTotal, labelValues));

            if (persistCountTotal > 0) {
                map.put("persist_cv_count_avg", new SimpleGauge.ValueWithLabelValues((double) persistCvCountTotal / persistCountTotal, labelValues));
                map.put("update_pvm_batch_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) updatePvmBatchCostTimeTotalUsTotal / persistCountTotal, labelValues));
            }

            return map;
        });
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

    byte[] preadForMerge(int targetSegmentIndex, int segmentCount) {
        if (segmentCount > FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT) {
            throw new IllegalArgumentException("Merge read segment count too large: " + segmentCount);
        }

        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentForMerge(segmentIndexTargetFd, segmentCount);
    }

    byte[] preadForRepl(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readOneInnerForRepl(segmentIndexTargetFd);
    }

    byte[] preadOneSegment(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readOneInner(segmentIndexTargetFd, true);
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

    boolean reuseSegments(boolean isFirstStart, int segmentCount, boolean updateAsReuseFlag) {
        // skip can not reuse segments
        if (!isFirstStart && segmentIndex == 0) {
            var segmentFlagList = oneSlot.getSegmentMergeFlagBatch(segmentIndex, segmentCount);
            int j = 0;
            for (int i = 0; i < segmentFlagList.size(); i++) {
                var segmentFlag = segmentFlagList.get(i);
                var flag = segmentFlag.flag();
                if (!flag.canReuse()) {
                    j = i;
                }
            }

            // begin with new segment index
            if (j != 0) {
                segmentIndex = j + 1;
                return reuseSegments(false, segmentCount, updateAsReuseFlag);
            }
        }

        for (int i = 0; i < segmentCount; i++) {
            var targetSegmentIndex = segmentIndex + i;

            var segmentFlag = oneSlot.getSegmentMergeFlag(targetSegmentIndex);
            var flag = segmentFlag.flag();

            // already set flag to reuse, can reuse
            if (flag == Flag.reuse) {
                continue;
            }

            // init can reuse
            if (flag == Flag.init) {
                if (updateAsReuseFlag) {
                    oneSlot.setSegmentMergeFlag(targetSegmentIndex, Flag.reuse, 0L, segmentFlag.walGroupIndex);
                }
                continue;
            }

            // merged and persisted, can reuse
            if (flag == Flag.merged_and_persisted) {
                if (updateAsReuseFlag) {
                    oneSlot.setSegmentMergeFlag(targetSegmentIndex, Flag.reuse, 0L, segmentFlag.walGroupIndex);
                }
                continue;
            }

            // left can not reuse: new_write, reuse_new, merging, merged
            log.warn("Chunk segment index is not init/merged and persisted/reuse, can not write, s={}, i={}, flag={}",
                    slot, targetSegmentIndex, flag);
            return false;
        }
        return true;
    }

    public enum Flag {
        init((byte) 100),
        new_write((byte) 0),
        reuse_new((byte) 10),
        merging((byte) 1),
        merged((byte) -1),
        merged_and_persisted((byte) -10),
        reuse((byte) -100);

        final byte flagByte;

        Flag(byte flagByte) {
            this.flagByte = flagByte;
        }

        static Flag fromFlagByte(byte flagByte) {
            for (var f : values()) {
                if (f.flagByte == flagByte) {
                    return f;
                }
            }
            throw new IllegalArgumentException("Flag not support: " + flagByte);
        }

        boolean canReuse() {
            return this == init || this == reuse || this == merged_and_persisted;
        }

        boolean isMergingOrMerged() {
            return this == merging || this == merged;
        }

        @Override
        public String toString() {
            return name() + "(" + flagByte + ")";
        }
    }

    public record SegmentFlag(Flag flag, long segmentSeq, int walGroupIndex) {
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
    // 64 not tight segments is enough for 1000 Wal.V persist ?, need check, todo
    // one wal group charges 64 key buckets, 64 * 4KB = 256KB
    // with merged valid cv list together, once read 10 segments, valid cv list may be > 1000
    public static int ONCE_PREPARE_SEGMENT_COUNT = 64;

    int mergedSegmentIndexEndLastTime = NO_NEED_MERGE_SEGMENT_INDEX;

    void checkMergedSegmentIndexEndLastTimeValidAfterServerStart() {
        if (mergedSegmentIndexEndLastTime != NO_NEED_MERGE_SEGMENT_INDEX) {
            if (mergedSegmentIndexEndLastTime < 0 || mergedSegmentIndexEndLastTime >= maxSegmentIndex) {
                throw new IllegalStateException("Merged segment index end last time out of bound, s=" + slot + ", i=" + mergedSegmentIndexEndLastTime);
            }
        }
    }

    // merge, valid cv list is smaller, only need one segment, or need 4 or 8 ? todo
    // but even list size is smaller, one v maybe is not compressed, one v encoded length is large
    public static int ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE = 16;

    // return need merge segment index array
    public ArrayList<Integer> persist(int walGroupIndex, ArrayList<Wal.V> list, boolean isMerge) {
        logMergeCount++;
        var doLog = (Debug.getInstance().logMerge && logMergeCount % 1000 == 0);

        moveIndexForPrepare();
        if (isMerge) {
            boolean canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE, false);
            if (!canWrite) {
                throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex);
            }
        } else {
            boolean canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT, false);
            if (!canWrite) {
                throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex);
            }
        }

        var oncePrepareSegmentCount = isMerge ? ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE : ONCE_PREPARE_SEGMENT_COUNT;
        int[] nextNSegmentIndex = new int[oncePrepareSegmentCount];
        for (int i = 0; i < oncePrepareSegmentCount; i++) {
            nextNSegmentIndex[i] = segmentIndex + i;
        }

        ArrayList<PersistValueMeta> pvmList = new ArrayList<>();
        var segments = segmentBatch.splitAndTight(list, nextNSegmentIndex, pvmList);

        List<Long> segmentSeqListAll = new ArrayList<>();
        for (var segment : segments) {
            segmentSeqListAll.add(segment.segmentSeq());
        }
        oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                Flag.reuse, segmentSeqListAll, walGroupIndex);

        boolean isNewAppendAfterBatch = true;

        var firstSegmentIndex = segments.getFirst().segmentIndex();
        var fdIndex = targetFdIndex(firstSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(firstSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];

        if (ConfForSlot.global.pureMemory) {
            isNewAppendAfterBatch = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            for (var segment : segments) {
                byte[] bytes = segment.tightBytesWithLength();
                fdReadWrite.writeOneInner(targetFdIndex(segment.segmentIndex()), bytes, false);

                if (masterUpdateCallback != null) {
                    List<Long> segmentSeqList = new ArrayList<>();
                    segmentSeqList.add(segment.segmentSeq());
                }
            }
            segmentIndex += segments.size();

            oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                    isNewAppendAfterBatch ? Flag.new_write : Flag.reuse_new, segmentSeqListAll, walGroupIndex);
        } else {
            if (segments.size() < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    List<Long> segmentSeqListSubBatch = new ArrayList<>();
                    segmentSeqListSubBatch.add(segment.segmentSeq());

                    boolean isNewAppend = writeSegments(segment.tightBytesWithLength(), 1);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);
                }
            } else {
                // batch write
                var batchCount = segments.size() / BATCH_ONCE_SEGMENT_COUNT_PWRITE;
                var remainCount = segments.size() % BATCH_ONCE_SEGMENT_COUNT_PWRITE;

                var tmpBatchBytes = new byte[chunkSegmentLength * BATCH_ONCE_SEGMENT_COUNT_PWRITE];
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
                        if (tightBytesWithLength.length < chunkSegmentLength) {
                            buffer.position(buffer.position() + chunkSegmentLength - tightBytesWithLength.length);
                        }

                        segmentSeqListSubBatch.add(segment.segmentSeq());
                    }

                    boolean isNewAppend = writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlagBatch(segmentIndex, BATCH_ONCE_SEGMENT_COUNT_PWRITE,
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segmentSeqListSubBatch, walGroupIndex);

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
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);
                }
            }
        }

        // stats
        persistCountTotal++;
        persistCvCountTotal += list.size();

        var beginT = System.nanoTime();
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList);
        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        updatePvmBatchCostTimeTotalUsTotal += costT;

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
            if (!needMergeSegmentIndexList.contains(0)) {
                throw new IllegalStateException("Need merge segment index list not contains 0 while reuse from the beginning, s="
                        + slot + ", list=" + needMergeSegmentIndexList);
            }
            if (mergedSegmentIndexEndLastTime == NO_NEED_MERGE_SEGMENT_INDEX) {
                throw new IllegalStateException("Merged segment index end last time not set, s=" + slot);
            }

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

            // mergedSegmentIndexEndLastTime maybe > firstNeedMergeSegmentIndex when server restart, because pre read merge before persist wal
//            assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
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
//                assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
                for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                    if (!needMergeSegmentIndexList.contains(i)) {
                        needMergeSegmentIndexList.add(i);
                    }
                }

                // last ONCE_PREPARE_SEGMENT_COUNT segments, need merge
                var lastNeedMergeSegmentIndex = needMergeSegmentIndexList.getLast();
                if (lastNeedMergeSegmentIndex >= maxSegmentIndex - ONCE_PREPARE_SEGMENT_COUNT) {
                    for (int i = lastNeedMergeSegmentIndex + 1; i <= maxSegmentIndex; i++) {
                        if (!needMergeSegmentIndexList.contains(i)) {
                            needMergeSegmentIndexList.add(i);
                        }
                    }
                    log.warn("Add extra need merge segment index to the end, s={}, i={}, list={}", slot, segmentIndex, needMergeSegmentIndexList);
                } else if (lastNeedMergeSegmentIndex < halfSegmentNumber &&
                        lastNeedMergeSegmentIndex >= halfSegmentNumber - 1 - ONCE_PREPARE_SEGMENT_COUNT) {
                    for (int i = lastNeedMergeSegmentIndex + 1; i < halfSegmentNumber; i++) {
                        if (!needMergeSegmentIndexList.contains(i)) {
                            needMergeSegmentIndexList.add(i);
                        }
                    }
                    log.warn("Add extra need merge segment index to the half end, s={}, i={}, list={}", slot, segmentIndex, needMergeSegmentIndexList);
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

        final int maxSize = ONCE_PREPARE_SEGMENT_COUNT * 4;

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
                    ", list size=" + list.size() +
                    ", max size=" + maxSize);
        }

        if (list.size() >= ONCE_PREPARE_SEGMENT_COUNT) {
            log.debug("Chunk persist need merge segment index list too large, performance bad, maybe many is skipped, s={}, i={}, list={}",
                    slot, segmentIndex, list);
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

    static final int NO_NEED_MERGE_SEGMENT_INDEX = -1;

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
                fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);

                oneSlot.setSegmentMergeFlag(segmentIndex,
                        isNewAppend ? Flag.new_write : Flag.reuse_new, segmentSeqList.getFirst(), walGroupIndex);
            } else {
                for (int i = 0; i < segmentCount; i++) {
                    var oneSegmentBytes = new byte[chunkSegmentLength];
                    System.arraycopy(bytes, i * chunkSegmentLength, oneSegmentBytes, 0, chunkSegmentLength);

                    fdReadWrite.writeOneInner(segmentIndexTargetFd + i, oneSegmentBytes, false);
                }

                oneSlot.setSegmentMergeFlagBatch(segmentIndex, segmentCount,
                        isNewAppend ? Flag.new_write : Flag.reuse_new, segmentSeqList, walGroupIndex);
            }

            return isNewAppend;
        } else {
            this.segmentIndex = segmentIndex;
            boolean isNewAppend = writeSegments(bytes, segmentCount);

            if (segmentCount == 1) {
                oneSlot.setSegmentMergeFlag(segmentIndex,
                        isNewAppend ? Flag.new_write : Flag.reuse_new, segmentSeqList.getFirst(), walGroupIndex);
            } else {
                oneSlot.setSegmentMergeFlagBatch(segmentIndex, segmentCount,
                        isNewAppend ? Flag.new_write : Flag.reuse_new, segmentSeqList, walGroupIndex);
            }

            return isNewAppend;
        }
    }

    private boolean writeSegments(byte[] bytes, int segmentCount) {
        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        if (segmentCount == 1) {
            fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
        } else if (segmentCount == BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
            fdReadWrite.writeSegmentBatch(segmentIndexTargetFd, bytes, false);
        } else if (segmentCount == REPL_ONCE_SEGMENT_COUNT) {
            fdReadWrite.writeOneInnerForRepl(segmentIndexTargetFd, bytes, 0);
        } else {
            throw new IllegalArgumentException("Write segment count not support: " + segmentCount);
        }

        boolean isNewAppend = false;
        int afterThisBatchOffset = (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
            isNewAppend = true;
        }

        return isNewAppend;
    }
}
