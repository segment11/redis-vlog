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

    private final Logger log = LoggerFactory.getLogger(getClass());

    public ChunkMergeJob(byte slot, ArrayList<Integer> needMergeSegmentIndexList, ChunkMergeWorker chunkMergeWorker, SnowFlake snowFlake) {
        this.slot = slot;
        this.needMergeSegmentIndexList = needMergeSegmentIndexList;
        this.chunkMergeWorker = chunkMergeWorker;
        this.oneSlot = chunkMergeWorker.oneSlot;
        this.snowFlake = snowFlake;
    }

    public int run() {
        // recycle, need spit to two part
        if (needMergeSegmentIndexList.getLast() - needMergeSegmentIndexList.getFirst() > oneSlot.chunk.halfSegmentNumber) {
            assert needMergeSegmentIndexList.contains(0);

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

            var job1 = new ChunkMergeJob(slot, onePart, chunkMergeWorker, snowFlake);
            var job2 = new ChunkMergeJob(slot, anotherPart, chunkMergeWorker, snowFlake);

            // first do job2, then do job1
            var validCvCount2 = job2.run();
            var validCvCount1 = job1.run();
            return validCvCount1 + validCvCount2;
        }

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

    void mergeSegments(List<Integer> needMergeSegmentIndexList) {
        chunkMergeWorker.logMergeCount++;
        var doLog = Debug.getInstance().logMerge && chunkMergeWorker.logMergeCount % 1000 == 0;

        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();
        assert needMergeSegmentIndexList.size() == lastSegmentIndex - firstSegmentIndex + 1;

        HashSet<Integer> skipSegmentIndexSet = new HashSet<>();
        for (var segmentIndex : needMergeSegmentIndexList) {
            var segmentFlag = oneSlot.getSegmentMergeFlag(segmentIndex);
            // not write yet, skip
            if (segmentFlag == null || segmentFlag.flag() == Chunk.SEGMENT_FLAG_INIT) {
                skipSegmentIndexSet.add(segmentIndex);
                continue;
            }

            var flag = segmentFlag.flag();
            if (flag == Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED) {
                skipSegmentIndexSet.add(segmentIndex);
                continue;
            }
        }

        chunkMergeWorker.lastMergedSegmentIndex = lastSegmentIndex;

        var allSkipped = needMergeSegmentIndexList.size() == skipSegmentIndexSet.size();
        if (allSkipped) {
            return;
        }

        chunkMergeWorker.mergedSegmentCount++;

        // read all segments to memory, then compare with key buckets
        int chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;
        var tmpCapacity = chunkSegmentLength / ConfForSlot.global.estimateOneValueLength * MERGE_READ_ONCE_SEGMENT_COUNT;
        ArrayList<CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(tmpCapacity);

        var beginT = System.nanoTime();
        var segmentBytesBatchRead = oneSlot.preadForMerge(firstSegmentIndex, MERGE_READ_ONCE_SEGMENT_COUNT);

        boolean alreadyDoLog = false;
        int i = 0;
        for (var segmentIndex : needMergeSegmentIndexList) {
            if (skipSegmentIndexSet.contains(segmentIndex)) {
                // move to next segment
                i++;
                continue;
            }

            int relativeOffsetInBatchBytes = i * chunkSegmentLength;
            // refer to Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // last segments not write at all, need skip
            if (segmentBytesBatchRead == null || relativeOffsetInBatchBytes >= segmentBytesBatchRead.length) {
                oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);
                if (doLog) {
                    log.info("Set segment flag to persisted as not write at all, s={}, i={}", slot, segmentIndex);
                }
                i++;
                continue;
            }

            oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGING, snowFlake.nextId());
            if (doLog && !alreadyDoLog) {
                log.info("Set segment flag to merging, s={}, i={}", slot, segmentIndex);
                alreadyDoLog = true;
            }

            readToCvList(cvList, segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength, segmentIndex, slot);
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
                one.bucketIndex = KeyHash.bucketIndex(one.cv.getKeyHash(), oneSlot.keyLoader.bucketsPerSlot);
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
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(one.cv, one.key, one.bucketIndex, one.segmentIndex));
            hasValidCvSegmentIndexSet.add(one.segmentIndex);
        }

        for (var segmentIndex : needMergeSegmentIndexList) {
            var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);

            if (hasValidCvSegmentIndexSet.contains(segmentIndex)) {
                oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGED, snowFlake.nextId());

                if (doLog) {
                    log.info("Set segment flag to merged, s={}, i={}, valid cv count={}, invalid cv count={}",
                            slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                }

                chunkMergeWorker.addMergedSegment(segmentIndex, validCvCountRecord.validCvCount);
            } else {
                oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);

                if (doLog && segmentIndex == firstSegmentIndex) {
                    log.info("Set segment flag to persisted, s={}, i={}, valid cv count={}, invalid cv count={}",
                            slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                }
            }
        }

        chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk();

        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        chunkMergeWorker.mergedSegmentCostTimeTotalUs += costT;

        chunkMergeWorker.validCvCountTotal += validCvCountAfterRun;
        chunkMergeWorker.invalidCvCountTotal += invalidCvCountAfterRun;
    }

    static void readToCvList(ArrayList<CvWithKeyAndSegmentOffset> cvList, byte[] segmentBytesBatchRead, int relativeOffsetInBatchBytes, int chunkSegmentLength, int segmentIndex, byte slot) {
        var buffer = ByteBuffer.wrap(segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength).slice();
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

            var decompressedBytes = new byte[chunkSegmentLength];
            var d = Zstd.decompressByteArray(decompressedBytes, 0, chunkSegmentLength,
                    segmentBytesBatchRead, relativeOffsetInBatchBytes + subBlockOffset, subBlockLength);
            if (d != chunkSegmentLength) {
                throw new IllegalStateException("Decompress error, s=" + slot
                        + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunkSegmentLength);
            }

            int finalSubBlockIndex = subBlockIndex;
            SegmentBatch.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));
            });
        }
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
