package redis.persist;

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static redis.persist.FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT;
import static redis.persist.LocalPersist.PAGE_SIZE;

public class ChunkMergeJob {
    private final byte slot;
    private final ArrayList<Integer> needMergeSegmentIndexList;
    private final ChunkMergeWorker chunkMergeWorker;
    private final OneSlot oneSlot;
    private final SnowFlake snowFlake;
    int validCvCountAfterRun = 0;
    int invalidCvCountAfterRun = 0;

    // for unit test
    int testTargetBucketIndex = -1;

    private final LocalPersist localPersist = LocalPersist.getInstance();

    private final Logger log = LoggerFactory.getLogger(getClass());

    public ChunkMergeJob(byte slot, ArrayList<Integer> needMergeSegmentIndexList, ChunkMergeWorker chunkMergeWorker, SnowFlake snowFlake) {
        this.slot = slot;
        this.needMergeSegmentIndexList = needMergeSegmentIndexList;
        this.chunkMergeWorker = chunkMergeWorker;
        this.oneSlot = chunkMergeWorker.oneSlot;
        this.snowFlake = snowFlake;
    }

    public int run() {
        var batchCount = needMergeSegmentIndexList.size() / MERGE_READ_ONCE_SEGMENT_COUNT;
        if (needMergeSegmentIndexList.size() % MERGE_READ_ONCE_SEGMENT_COUNT != 0) {
            batchCount++;
        }
        try {
            for (int i = 0; i < batchCount; i++) {
                var subList = needMergeSegmentIndexList.subList(i * MERGE_READ_ONCE_SEGMENT_COUNT,
                        Math.min((i + 1) * MERGE_READ_ONCE_SEGMENT_COUNT, needMergeSegmentIndexList.size()));
                mergeSegments(subList);
            }
            return validCvCountAfterRun;
        } catch (Exception e) {
            log.error("Merge chunk error, s={}, i={}, message={}", slot, needMergeSegmentIndexList, e.getMessage());
            log.error("Merge chunk error", e);
            return -1;
        }
    }

    public static class CvWithKeyAndSegmentOffset {
        public CvWithKeyAndSegmentOffset(CompressedValue cv, String key, int segmentOffset, int segmentIndex, byte subBlockIndex) {
            this.cv = cv;
            this.key = key;
            this.segmentOffset = segmentOffset;
            this.segmentIndex = segmentIndex;
            this.subBlockIndex = subBlockIndex;
        }

        public CompressedValue cv;
        public final String key;
        final int segmentOffset;
        final int segmentIndex;
        final byte subBlockIndex;

        // calc for batch pread key buckets, for performance
        int bucketIndex;
        int walGroupIndex;

        public String shortString() {
            return "k=" + key + ", s=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
        }

        @Override
        public String toString() {
            return "CvWithKeyAndSegmentOffset{" +
                    "key='" + key + '\'' +
                    ", segmentOffset=" + segmentOffset +
                    ", segmentIndex=" + segmentIndex +
                    ", subBlockIndex=" + subBlockIndex +
                    '}';
        }
    }

    private static class ValidCvCountRecord {
        ValidCvCountRecord(int segmentIndex) {
            this.segmentIndex = segmentIndex;
        }

        int validCvCount;
        int invalidCvCount;
        final int segmentIndex;
    }

    private long logMergeCount = 0;

    void mergeSegments(List<Integer> needMergeSegmentIndexList) {
        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();
        assert needMergeSegmentIndexList.size() == lastSegmentIndex - firstSegmentIndex + 1;

        int segmentLength = ConfForSlot.global.confChunk.segmentLength;
        var npages0 = segmentLength / PAGE_SIZE;
        int npagesMerge = npages0 * MERGE_READ_ONCE_SEGMENT_COUNT;

        HashSet<Integer> skipSegmentIndexSet = new HashSet<>();
        for (var segmentIndex : needMergeSegmentIndexList) {
            var segmentFlag = oneSlot.getSegmentMergeFlag(segmentIndex);
            // not write yet, skip
            if (segmentFlag == null || segmentFlag.flag() == Chunk.SEGMENT_FLAG_INIT) {
                skipSegmentIndexSet.add(segmentIndex);
                break;
            }

            var flag = segmentFlag.flag();
            if (flag == Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED) {
                skipSegmentIndexSet.add(segmentIndex);
                break;
            }

            if (flag == Chunk.SEGMENT_FLAG_MERGED) {
                // if in merge chunk batch for very long time and not persisted yet, means valid cv count is too small
                // need a persist trigger
                // this chunk batch is already merged by other worker, skip
                skipSegmentIndexSet.add(segmentIndex);
                break;
            }

            // flag == Chunk.SEGMENT_FLAG_REUSE:
            // do nothing
            // write segment increase mush faster than merge segment (diff 16384+), merge executor already reject
            // will never happen here

            // flag == Chunk.SEGMENT_FLAG_MERGING:
            // do nothing
            // when server start, already recover segment flag
            // maybe crash before
            // continue to merge
        }

        var allSkipped = needMergeSegmentIndexList.size() == skipSegmentIndexSet.size();
        if (allSkipped) {
            return;
        }

        chunkMergeWorker.lastMergedSegmentIndex = lastSegmentIndex;

        chunkMergeWorker.mergedSegmentCount++;

        var beginT = System.nanoTime();
        var segmentBytesBatchRead = oneSlot.preadForMerge(firstSegmentIndex);

        var doLog = Debug.getInstance().logMerge;
        if (doLog) {
            if (logMergeCount % 100 != 0) {
                doLog = false;
            }
            logMergeCount++;
        }

        // read all segments to memory, then compare with key buckets
        ArrayList<CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(npagesMerge * 20);

        int i = 0;
        for (var segmentIndex : needMergeSegmentIndexList) {
            if (skipSegmentIndexSet.contains(segmentIndex)) {
                // move to next segment
                i++;
                continue;
            }

            oneSlot.setSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGING, snowFlake.nextId());
            if (doLog) {
                log.info("Set segment flag to merging, s={}, i={}", slot, segmentIndex);
            }

            int relativeOffsetInBatchBytes = i * segmentLength;
            var buffer = ByteBuffer.wrap(segmentBytesBatchRead, relativeOffsetInBatchBytes, segmentLength).slice();
            // sub blocks
            // refer to SegmentBatch tight HEADER_LENGTH
            for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
                // position to target sub block
                buffer.position(SegmentBatch.subBlockMetaPosition(subBlockIndex));
                var subBlockOffset = buffer.getShort();
                if (subBlockOffset == 0) {
                    break;
                }
                var subBlockLength = buffer.getShort();

                var decompressedBytes = new byte[segmentLength];
                var d = Zstd.decompressByteArray(decompressedBytes, 0, segmentLength,
                        segmentBytesBatchRead, relativeOffsetInBatchBytes + subBlockOffset, subBlockLength);
                if (d != segmentLength) {
                    throw new IllegalStateException("Decompress error, s=" + slot
                            + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", segmentLength=" + segmentLength);
                }

                int finalSubBlockIndex = subBlockIndex;
                SegmentBatch.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                    cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));
                });
            }

            i++;
        }

        ArrayList<ValidCvCountRecord> validCvCountRecordList = new ArrayList<>(needMergeSegmentIndexList.size());
        for (var segmentIndex : needMergeSegmentIndexList) {
            validCvCountRecordList.add(new ValidCvCountRecord(segmentIndex));
        }

        // calc bucket index and wal group index
        for (var one : cvList) {
            if (testTargetBucketIndex != -1) {
                one.bucketIndex = testTargetBucketIndex;
            } else {
                one.bucketIndex = localPersist.bucketIndex(one.cv.getKeyHash());
            }
            one.walGroupIndex = Wal.calWalGroupIndex(one.bucketIndex);
        }
        // compare to current wal or persist value meta in key buckets, remove those deleted or expired
        removeOld(oneSlot, cvList, validCvCountRecordList);

        for (var validCvCountRecord : validCvCountRecordList) {
            validCvCountAfterRun += validCvCountRecord.validCvCount;
            invalidCvCountAfterRun += validCvCountRecord.invalidCvCount;
        }

        HashSet<Integer> hasValidCvSegmentIndexSet = new HashSet<>();
        for (var one : cvList) {
            // use memory list, and threshold, then persist to merge worker's chunk
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndex(one.cv, one.key, one.bucketIndex));
            hasValidCvSegmentIndexSet.add(one.segmentIndex);
        }

        boolean isPersisted = chunkMergeWorker.persistMergedCvList();
        if (isPersisted) {
            for (var segmentIndex : needMergeSegmentIndexList) {
                oneSlot.setSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);

                if (doLog) {
                    var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                    log.info("Set segment flag to persisted, s={}, i={}, valid cv count={}, invalid cv count={}",
                            slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                }
            }
        } else {
            for (var segmentIndex : needMergeSegmentIndexList) {
                var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                if (hasValidCvSegmentIndexSet.contains(segmentIndex)) {
                    oneSlot.setSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGED, snowFlake.nextId());

                    if (doLog) {
                        log.info("Set segment flag to merged, s={}, i={}, valid cv count={}, invalid cv count={}",
                                slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                    }

                    // add to merged set
                    chunkMergeWorker.mergedSegmentSet.add(new ChunkMergeWorker.MergedSegment(segmentIndex, validCvCountRecord.validCvCount));
                } else {
                    oneSlot.setSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);

                    if (doLog) {
                        log.info("Set segment flag to persisted, s={}, i={}, valid cv count={}, invalid cv count={}",
                                slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                    }
                }
            }
        }

        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        chunkMergeWorker.mergedSegmentCostTimeTotalUs += costT;

        chunkMergeWorker.validCvCountTotal += validCvCountAfterRun;
        chunkMergeWorker.invalidCvCountTotal += invalidCvCountAfterRun;
    }

    private void removeOld(OneSlot oneSlot, ArrayList<CvWithKeyAndSegmentOffset> cvList, ArrayList<ValidCvCountRecord> validCvCountRecordList) {
        var dictMap = DictMap.getInstance();
        var firstSegmentIndex = validCvCountRecordList.get(0).segmentIndex;
        ArrayList<CvWithKeyAndSegmentOffset> toRemoveCvList = new ArrayList<>(cvList.size());

        var groupByWalGroupIndex = cvList.stream().collect(Collectors.groupingBy(one -> one.walGroupIndex));
        for (var entry : groupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var keyBucketsInOneWalGroup = new KeyBucketsInOneWalGroup(slot, walGroupIndex, oneSlot.keyLoader);

            var cvListInOneWalGroup = entry.getValue();
            for (var one : cvListInOneWalGroup) {
                var key = one.key;
                var cv = one.cv;
                var segmentOffset = one.segmentOffset;
                var segmentIndex = one.segmentIndex;
                var subBlockIndex = one.subBlockIndex;
                var bucketIndex = one.bucketIndex;

                var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);

                byte[] valueBytesCurrent;
                var tmpWalValueBytes = oneSlot.getFromWal(key, bucketIndex);
                if (tmpWalValueBytes != null) {
                    // wal kv is the newest
                    // need not thread safe, because newest seq must > cv seq
                    if (CompressedValue.isDeleted(tmpWalValueBytes)) {
                        valueBytesCurrent = null;
                    } else {
                        valueBytesCurrent = tmpWalValueBytes;
                    }
                } else {
                    var valueBytesWithExpireAtAndSeq = keyBucketsInOneWalGroup.getValue(bucketIndex, key.getBytes(), cv.getKeyHash());
                    if (valueBytesWithExpireAtAndSeq == null || valueBytesWithExpireAtAndSeq.isExpired()) {
                        valueBytesCurrent = null;
                    } else {
                        if (valueBytesWithExpireAtAndSeq.seq() != cv.getSeq()) {
                            valueBytesCurrent = null;
                        } else {
                            valueBytesCurrent = valueBytesWithExpireAtAndSeq.valueBytes();
                        }
                    }
                }
                if (valueBytesCurrent == null) {
                    validCvCountRecord.invalidCvCount++;
                    toRemoveCvList.add(one);
                    continue;
                }

                // if not meta
                if (!PersistValueMeta.isPvm(valueBytesCurrent)) {
                    // compare seq
                    var buffer = ByteBuffer.wrap(valueBytesCurrent);

                    long valueSeqCurrent;
                    var firstByte = buffer.get(0);
                    // from write batch, maybe short value
                    if (firstByte < 0) {
                        valueSeqCurrent = buffer.position(1).getLong();
                    } else {
                        // normal compressed value encoded
                        valueSeqCurrent = buffer.getLong();
                    }

                    if (cv.getSeq() < valueSeqCurrent) {
                        // cv is old， discard
                        validCvCountRecord.invalidCvCount++;
                        toRemoveCvList.add(one);
                    } else {
                        // this else block will never happen
                        // because compressed value is newest from write batch kv
                        // from key loader, must be persisted value meta
                        throw new IllegalStateException("Merge compressed value is newer than write batch compressed value, s=" + slot +
                                ", i=" + segmentIndex + ", key=" + key + ", merge cv seq=" + cv.getSeq() +
                                ", write batch cv seq=" + valueSeqCurrent);
                    }
                } else {
                    var pvmCurrent = PersistValueMeta.decode(valueBytesCurrent);
                    if (pvmCurrent.segmentIndex != segmentIndex || pvmCurrent.subBlockIndex != subBlockIndex || pvmCurrent.segmentOffset != segmentOffset) {
                        // cv is old， discard
                        validCvCountRecord.invalidCvCount++;
                        toRemoveCvList.add(one);
                    } else {
                        if (cv.isExpired()) {
                            validCvCountRecord.invalidCvCount++;
                            toRemoveCvList.add(one);

                            if (cv.isBigString()) {
                                // need remove file
                                var buffer = ByteBuffer.wrap(cv.getCompressedData());
                                var uuid = buffer.getLong();

                                var isDeleted = oneSlot.getBigStringFiles().deleteBigStringFileIfExist(uuid);
                                if (!isDeleted) {
                                    throw new RuntimeException("Delete big string file error, s=" + slot +
                                            ", i=" + segmentIndex + ", key=" + key + ", uuid=" + uuid);
                                } else {
                                    log.warn("Delete big string file, s={}, i={}, key={}, uuid={}",
                                            slot, segmentIndex, key, uuid);
                                }
                            }
                        } else {
                            validCvCountRecord.validCvCount++;

                            // if there is a new dict, compress use new dict and replace
                            if (cv.isUseDict() && cv.getDictSeqOrSpType() == Dict.SELF_ZSTD_DICT_SEQ) {
                                var dict = dictMap.getDict(TrainSampleJob.keyPrefix(key));
                                if (dict != null) {
                                    var rawBytes = cv.decompress(Dict.SELF_ZSTD_DICT);
                                    var newCompressedCv = CompressedValue.compress(rawBytes, dict, chunkMergeWorker.compressLevel);
                                    if (newCompressedCv.compressedLength() < cv.compressedLength()) {
                                        // replace
                                        newCompressedCv.setSeq(cv.getSeq());
                                        newCompressedCv.setDictSeqOrSpType(dict.getSeq());
                                        newCompressedCv.setKeyHash(cv.getKeyHash());
                                        newCompressedCv.setExpireAt(cv.getExpireAt());

                                        one.cv = newCompressedCv;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        cvList.removeAll(toRemoveCvList);
    }
}
