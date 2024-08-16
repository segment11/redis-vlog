package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.Debug;
import redis.SnowFlake;
import redis.metric.SimpleGauge;
import redis.repl.incremental.XOneWalGroupPersist;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static redis.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE;
import static redis.persist.FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;

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

    @Override
    public String toString() {
        return "Chunk{" +
                "slot=" + slot +
                ", segmentNumberPerFd=" + segmentNumberPerFd +
                ", fdPerChunk=" + fdPerChunk +
                ", maxSegmentIndex=" + maxSegmentIndex +
                ", halfSegmentNumber=" + halfSegmentNumber +
                ", chunkSegmentLength=" + chunkSegmentLength +
                ", segmentIndex=" + segmentIndex +
                '}';
    }

    final static SimpleGauge chunkPersistGauge = new SimpleGauge("chunk_persist", "chunk persist segments",
            "slot");

    static {
        chunkPersistGauge.register();
    }

    long persistCountTotal;
    long persistCvCountTotal;
    long updatePvmBatchCostTimeTotalUsTotal;

    final OneSlot oneSlot;
    private final KeyLoader keyLoader;
    private final SegmentBatch segmentBatch;

    int[] fdLengths;
    FdReadWrite[] fdReadWriteArray;

    public Chunk(byte slot, File slotDir, OneSlot oneSlot,
                 SnowFlake snowFlake, KeyLoader keyLoader) {
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
        if (segmentCount > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE) {
            throw new IllegalArgumentException("Merge read segment count too large: " + segmentCount);
        }

        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readSegmentsForMerge(segmentIndexTargetFd, segmentCount);
    }

    byte[] preadForRepl(int targetSegmentIndex) {
        var fdIndex = targetFdIndex(targetSegmentIndex);
        var segmentIndexTargetFd = targetSegmentIndexTargetFd(targetSegmentIndex);

        var fdReadWrite = fdReadWriteArray[fdIndex];
        return fdReadWrite.readBatchForRepl(segmentIndexTargetFd);
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

    public int currentSegmentIndex() {
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

        public byte flagByte() {
            return flagByte;
        }

        Flag(byte flagByte) {
            this.flagByte = flagByte;
        }

        public static Flag fromFlagByte(byte flagByte) {
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

    // for find bug
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
    public ArrayList<Integer> persist(int walGroupIndex, ArrayList<Wal.V> list, boolean isMerge, XOneWalGroupPersist xForBinlog) {
        logMergeCount++;
        var doLog = (Debug.getInstance().logMerge && logMergeCount % 1000 == 0);

        moveSegmentIndexForPrepare();
        boolean canWrite;
        if (isMerge) {
            canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE, false);
        } else {
            canWrite = reuseSegments(false, ONCE_PREPARE_SEGMENT_COUNT, false);
        }
        if (!canWrite) {
            throw new SegmentOverflowException("Segment can not write, s=" + slot + ", i=" + segmentIndex);
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

        // never cross fd files because prepare batch segments to write
        var fdReadWrite = fdReadWriteArray[fdIndex];

        if (ConfForSlot.global.pureMemory) {
            isNewAppendAfterBatch = fdReadWrite.isTargetSegmentIndexNullInMemory(segmentIndexTargetFd);
            for (var segment : segments) {
                var bytes = segment.tightBytesWithLength();
                fdReadWrite.writeOneInner(targetSegmentIndexTargetFd(segment.segmentIndex()), bytes, false);

                xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                        isNewAppendAfterBatch ? Flag.new_write : Flag.reuse_new, segment.segmentSeq());
            }

            oneSlot.setSegmentMergeFlagBatch(segmentIndex, segments.size(),
                    isNewAppendAfterBatch ? Flag.new_write : Flag.reuse_new, segmentSeqListAll, walGroupIndex);

            moveSegmentIndexNext(segments.size());
        } else {
            if (segments.size() < BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
                for (var segment : segments) {
                    var bytes = segment.tightBytesWithLength();
                    boolean isNewAppend = writeSegments(bytes, 1);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlag(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segment.segmentSeq(), walGroupIndex);

                    moveSegmentIndexNext(1);

                    xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segment.segmentSeq());
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
                        var bytes = segment.tightBytesWithLength();
                        buffer.put(bytes);

                        // padding to segment length
                        if (bytes.length < chunkSegmentLength) {
                            buffer.position(buffer.position() + chunkSegmentLength - bytes.length);
                        }

                        segmentSeqListSubBatch.add(segment.segmentSeq());

                        xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                    }

                    boolean isNewAppend = writeSegments(tmpBatchBytes, BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                    isNewAppendAfterBatch = isNewAppend;

                    // need set segment flag so that merge worker can merge
                    oneSlot.setSegmentMergeFlagBatch(segmentIndex, BATCH_ONCE_SEGMENT_COUNT_PWRITE,
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segmentSeqListSubBatch, walGroupIndex);

                    for (int j = 0; j < BATCH_ONCE_SEGMENT_COUNT_PWRITE; j++) {
                        var segment = segments.get(i * BATCH_ONCE_SEGMENT_COUNT_PWRITE + j);

                        xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                                isNewAppend ? Flag.new_write : Flag.reuse_new, segment.segmentSeq());
                    }

                    moveSegmentIndexNext(BATCH_ONCE_SEGMENT_COUNT_PWRITE);
                }

                for (int i = 0; i < remainCount; i++) {
                    var segment = segments.get(batchCount * BATCH_ONCE_SEGMENT_COUNT_PWRITE + i);
                    var bytes = segment.tightBytesWithLength();
                    boolean isNewAppend = writeSegments(bytes, 1);
                    isNewAppendAfterBatch = isNewAppend;

                    xForBinlog.putUpdatedChunkSegmentBytes(segment.segmentIndex(), bytes);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segment.segmentIndex(),
                            isNewAppend ? Flag.new_write : Flag.reuse_new, segment.segmentSeq());

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
        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, pvmList, xForBinlog);
        var costT = (System.nanoTime() - beginT) / 1000;
        updatePvmBatchCostTimeTotalUsTotal += costT;

        // update meta, segment index for next time
        oneSlot.setMetaChunkSegmentIndex(segmentIndex);
        xForBinlog.setChunkSegmentIndexAfterPersist(segmentIndex);

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
        updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList);

        if (doLog) {
            log.info("Chunk persist need merge segment index list, s={}, i={}, list={}", slot, segmentIndex, needMergeSegmentIndexList);
        }
        return needMergeSegmentIndexList;
    }

    void updateLastMergedSegmentIndexEnd(ArrayList<Integer> needMergeSegmentIndexList) {
        TreeSet<Integer> sorted = new TreeSet<>(needMergeSegmentIndexList);

        // recycle, need spit to two part
        if (sorted.getLast() - sorted.getFirst() > halfSegmentNumber) {
            if (!sorted.contains(0)) {
                throw new IllegalStateException("Need merge segment index list not contains 0 while reuse from the beginning, s="
                        + slot + ", list=" + sorted);
            }
            if (mergedSegmentIndexEndLastTime == NO_NEED_MERGE_SEGMENT_INDEX) {
                throw new IllegalStateException("Merged segment index end last time not set, s=" + slot);
            }

            TreeSet<Integer> onePart = new TreeSet<>();
            TreeSet<Integer> anotherPart = new TreeSet<>();
            for (var segmentIndex : sorted) {
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

            // mergedSegmentIndexEndLastTime maybe > firstNeedMergeSegmentIndex when server restart, because pre-read merge before persist wal
//            assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
            for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                anotherPart.add(i);
            }

            checkNeedMergeSegmentIndexListContinuous(onePart);
            checkNeedMergeSegmentIndexListContinuous(anotherPart);

            mergedSegmentIndexEndLastTime = onePart.getLast();
        } else {
            var firstNeedMergeSegmentIndex = sorted.getFirst();
            if (mergedSegmentIndexEndLastTime == NO_NEED_MERGE_SEGMENT_INDEX) {
                if (firstNeedMergeSegmentIndex != 0) {
                    throw new IllegalStateException("First need merge segment index not 0, s=" + slot + ", i=" + firstNeedMergeSegmentIndex);
                }
            } else {
                // prepend from merged segment index end last time
//                assert mergedSegmentIndexEndLastTime < firstNeedMergeSegmentIndex;
                for (int i = mergedSegmentIndexEndLastTime + 1; i < firstNeedMergeSegmentIndex; i++) {
                    sorted.add(i);
                }

                // last ONCE_PREPARE_SEGMENT_COUNT segments, need merge
                var lastNeedMergeSegmentIndex = sorted.getLast();
                if (lastNeedMergeSegmentIndex >= maxSegmentIndex - ONCE_PREPARE_SEGMENT_COUNT) {
                    for (int i = lastNeedMergeSegmentIndex + 1; i <= maxSegmentIndex; i++) {
                        sorted.add(i);
                    }
                    log.warn("Add extra need merge segment index to the end, s={}, i={}, list={}", slot, segmentIndex, sorted);
                } else if (lastNeedMergeSegmentIndex < halfSegmentNumber &&
                        lastNeedMergeSegmentIndex >= halfSegmentNumber - 1 - ONCE_PREPARE_SEGMENT_COUNT) {
                    for (int i = lastNeedMergeSegmentIndex + 1; i < halfSegmentNumber; i++) {
                        sorted.add(i);
                    }
                    log.warn("Add extra need merge segment index to the half end, s={}, i={}, list={}", slot, segmentIndex, sorted);
                }
            }

            checkNeedMergeSegmentIndexListContinuous(sorted);
            mergedSegmentIndexEndLastTime = sorted.getLast();
        }
    }

    void checkNeedMergeSegmentIndexListContinuous(TreeSet<Integer> sortedSet) {
        if (sortedSet.size() == 1) {
            return;
        }

        final int maxSize = ONCE_PREPARE_SEGMENT_COUNT * 4;

        if (sortedSet.getLast() - sortedSet.getFirst() != sortedSet.size() - 1) {
            throw new IllegalStateException("Need merge segment index not continuous, s=" + slot +
                    ", first need merge segment index=" + sortedSet.getFirst() +
                    ", last need merge segment index=" + sortedSet.getLast() +
                    ", last time merged segment index =" + mergedSegmentIndexEndLastTime +
                    ", list size=" + sortedSet.size());
        }

        if (sortedSet.size() > maxSize) {
            throw new IllegalStateException("Need merge segment index list size too large, s=" + slot +
                    ", first need merge segment index=" + sortedSet.getFirst() +
                    ", last need merge segment index=" + sortedSet.getLast() +
                    ", last time merged segment index =" + mergedSegmentIndexEndLastTime +
                    ", list size=" + sortedSet.size() +
                    ", max size=" + maxSize);
        }

        if (sortedSet.size() >= ONCE_PREPARE_SEGMENT_COUNT) {
            log.debug("Chunk persist need merge segment index list too large, performance bad, maybe many is skipped, s={}, i={}, list={}",
                    slot, segmentIndex, sortedSet);
        }
    }

    private long logMergeCount = 0;

    private void moveSegmentIndexForPrepare() {
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

    public void writeSegmentsFromMasterExists(byte[] bytes, int segmentIndex, int segmentCount) {
        if (ConfForSlot.global.pureMemory) {
            var fdIndex = targetFdIndex(segmentIndex);
            var segmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

            var fdReadWrite = fdReadWriteArray[fdIndex];
            if (segmentCount == 1) {
                fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
            } else {
                for (int i = 0; i < segmentCount; i++) {
                    var oneSegmentBytes = new byte[chunkSegmentLength];
                    System.arraycopy(bytes, i * chunkSegmentLength, oneSegmentBytes, 0, chunkSegmentLength);

                    fdReadWrite.writeOneInner(segmentIndexTargetFd + i, oneSegmentBytes, false);
                }
            }
        } else {
            this.segmentIndex = segmentIndex;
            writeSegmentsForRepl(bytes, segmentCount);
        }
    }

    public boolean writeSegmentToTargetSegmentIndex(byte[] bytes, int targetSegmentIndex) {
        this.segmentIndex = targetSegmentIndex;
        return writeSegments(bytes, 1);
    }

    private boolean writeSegments(byte[] bytes, int segmentCount) {
//        if (segmentCount != 1 && segmentCount != BATCH_ONCE_SEGMENT_COUNT_PWRITE) {
//            throw new IllegalArgumentException("Write segment count not support: " + segmentCount);
//        }

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        if (segmentCount == 1) {
            fdReadWrite.writeOneInner(segmentIndexTargetFd, bytes, false);
        } else {
            fdReadWrite.writeSegmentsBatch(segmentIndexTargetFd, bytes, false);
        }

        boolean isNewAppend = false;
        int afterThisBatchOffset = (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
            isNewAppend = true;
        }

        return isNewAppend;
    }

    private void writeSegmentsForRepl(byte[] bytes, int segmentCount) {
        if (segmentCount > REPL_ONCE_SEGMENT_COUNT_PREAD) {
            throw new IllegalArgumentException("Write segment count not support: " + segmentCount);
        }

        var fdIndex = targetFdIndex();
        var segmentIndexTargetFd = targetSegmentIndexTargetFd();

        var fdReadWrite = fdReadWriteArray[fdIndex];
        fdReadWrite.writeSegmentsBatchForRepl(segmentIndexTargetFd, bytes);

        int afterThisBatchOffset = (segmentIndexTargetFd + segmentCount) * chunkSegmentLength;
        if (fdLengths[fdIndex] < afterThisBatchOffset) {
            fdLengths[fdIndex] = afterThisBatchOffset;
        }
    }
}
