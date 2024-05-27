package redis.persist;

import com.github.luben.zstd.Zstd;
import io.activej.common.function.SupplierEx;
import redis.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static redis.persist.FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT;
import static redis.persist.LocalPersist.PAGE_SIZE;

public class ChunkMergeJob implements SupplierEx<Integer> {
    byte workerId;
    byte slot;
    byte batchIndex;
    ArrayList<Integer> needMergeSegmentIndexList;
    ChunkMergeWorker mergeWorker;
    SnowFlake snowFlake;
    int validCvCountAfterRun = 0;
    int invalidCvCountAfterRun = 0;

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public Integer get() {
        boolean isTopMergeWorkerSelfMerge = workerId == mergeWorker.mergeWorkerId;

        var batchCount = needMergeSegmentIndexList.size() / MERGE_READ_ONCE_SEGMENT_COUNT;
        if (needMergeSegmentIndexList.size() % MERGE_READ_ONCE_SEGMENT_COUNT != 0) {
            batchCount++;
        }
        try {
            for (int i = 0; i < batchCount; i++) {
                var subList = needMergeSegmentIndexList.subList(i * MERGE_READ_ONCE_SEGMENT_COUNT,
                        Math.min((i + 1) * MERGE_READ_ONCE_SEGMENT_COUNT, needMergeSegmentIndexList.size()));
                mergeSegments(isTopMergeWorkerSelfMerge, subList);
            }
            return validCvCountAfterRun;
        } catch (Exception e) {
            mergeWorker.log.error("Merge chunk error, w={}, s={}, b={}, i={}, mw={}, message={}", workerId, slot, batchIndex,
                    needMergeSegmentIndexList, mergeWorker.mergeWorkerId, e.getMessage());
            mergeWorker.log.error("Merge chunk error", e);
            return -1;
        }
    }

    private static class CvWithKeyAndSegmentOffset {
        public CvWithKeyAndSegmentOffset(CompressedValue cv, String key, int segmentOffset, int segmentIndex, byte subBlockIndex) {
            this.cv = cv;
            this.key = key;
            this.segmentOffset = segmentOffset;
            this.segmentIndex = segmentIndex;
            this.subBlockIndex = subBlockIndex;
        }

        CompressedValue cv;
        final String key;
        final long segmentOffset;
        final int segmentIndex;
        final byte subBlockIndex;

        // calc for batch pread key buckets, for performance
        int bucketIndex;
        int walGroupIndex;
    }

    private static class ValidCvCountRecord {
        ValidCvCountRecord(int segmentIndex) {
            this.segmentIndex = segmentIndex;
        }

        int validCvCount;
        int invalidCvCount;
        final int segmentIndex;
    }

    private void mergeSegments(boolean isTopMergeWorkerSelfMerge, List<Integer> needMergeSegmentIndexList) {
        if (mergeWorker.isTopMergeWorker) {
            mergeWorker.log.debug("Add debug point here, w={}, s={}, mw={}", workerId, slot, mergeWorker.mergeWorkerId);
        }

        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();
        assert needMergeSegmentIndexList.size() == lastSegmentIndex - firstSegmentIndex + 1;

        int segmentLength = ConfForSlot.global.confChunk.segmentLength;
        var npages0 = segmentLength / PAGE_SIZE;
        int npagesMerge = npages0 * MERGE_READ_ONCE_SEGMENT_COUNT;

        var oneSlot = localPersist.oneSlot(slot);

        HashSet<Integer> skipSegmentIndexSet = new HashSet<>();
        for (var segmentIndex : needMergeSegmentIndexList) {
            var segmentFlag = oneSlot.getSegmentMergeFlag(workerId, batchIndex, segmentIndex);
            // not write yet, skip
            if (segmentFlag == null || segmentFlag.flag() == Chunk.SEGMENT_FLAG_INIT) {
                skipSegmentIndexSet.add(segmentIndex);
                break;
            }

            var flag = segmentFlag.flag();
            // top merge worker force merge ignore flag
            if (flag == Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED && !isTopMergeWorkerSelfMerge) {
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

        mergeWorker.lastMergedWorkerId = workerId;
        mergeWorker.lastMergedSlot = slot;
        mergeWorker.lastMergedSegmentIndex = lastSegmentIndex;

        mergeWorker.mergedSegmentCount++;

        var beginT = System.nanoTime();
        var segmentBytesBatchRead = oneSlot.preadForMerge(workerId, batchIndex, firstSegmentIndex);

        // only log slot 0, just for less log
        var doLog = (isTopMergeWorkerSelfMerge && firstSegmentIndex % 1000 == 0 && slot == 0) ||
                (mergeWorker.isTopMergeWorker && !isTopMergeWorkerSelfMerge && firstSegmentIndex % 10000 == 0 && slot == 0) ||
                (firstSegmentIndex % 10000 == 0 && slot == 0) ||
                Debug.getInstance().logMerge;

        // read all segments to memory, then compare with key buckets
        ArrayList<CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(npagesMerge * 20);

        int i = 0;
        for (var segmentIndex : needMergeSegmentIndexList) {
            if (skipSegmentIndexSet.contains(segmentIndex)) {
                // move to next segment
                i++;
                continue;
            }

            oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGING, mergeWorker.mergeWorkerId, snowFlake.nextId());
            if (doLog) {
                mergeWorker.log.info("Set segment flag to merging, w={}, s={}, b={}, i={}, mw={}", workerId, slot, batchIndex, segmentIndex,
                        mergeWorker.mergeWorkerId);
            }

            int relativeOffsetInBatchBytes = i * segmentLength;
            var buffer = ByteBuffer.wrap(segmentBytesBatchRead, relativeOffsetInBatchBytes, segmentLength);
            // sub blocks
            // refer to SegmentBatch tight HEADER_LENGTH
            for (int j = 0; j < SegmentBatch.MAX_BLOCK_NUMBER; j++) {
                // seq long + total bytes length int + each sub block * (offset short + length short)
                // position to target sub block
                buffer.position(8 + 4 + j * (2 + 2));
                var subBlockOffset = buffer.getShort();
                if (subBlockOffset == 0) {
                    break;
                }

                var subBlockLength = buffer.getShort();

                var uncompressedBytes = new byte[segmentLength];
                var d = Zstd.decompressByteArray(uncompressedBytes, 0, segmentLength,
                        segmentBytesBatchRead, relativeOffsetInBatchBytes + subBlockOffset, subBlockLength);
                if (d != segmentLength) {
                    throw new IllegalStateException("Decompress error, w=" + workerId + ", s=" + slot +
                            ", b=" + batchIndex + ", i=" + segmentIndex + ", sbi=" + j + ", d=" + d + ", segmentLength=" + segmentLength);
                }

                int finalJ = j;
                SegmentBatch.iterateFromSegmentBytes(uncompressedBytes, (key, cv, offsetInThisSegment) -> {
                    cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalJ));
                });
            }

            i++;
        }

        ArrayList<ValidCvCountRecord> validCvCountRecordList = new ArrayList<>(needMergeSegmentIndexList.size());
        for (var segmentIndex : needMergeSegmentIndexList) {
            validCvCountRecordList.add(new ValidCvCountRecord(segmentIndex));
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
            mergeWorker.addMergedCv(slot, batchIndex, new ChunkMergeWorker.CvWithKey(one.cv, one.key));
            hasValidCvSegmentIndexSet.add(one.segmentIndex);
        }

        boolean isPersisted = mergeWorker.persistMergedCvList(slot, batchIndex);
        if (isPersisted) {
            for (var segmentIndex : needMergeSegmentIndexList) {
                oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, mergeWorker.mergeWorkerId, 0L);

                if (doLog) {
                    var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                    mergeWorker.log.info("Set segment flag to persisted, w={}, s={}, b={}, i={}, mw={}, valid cv count={}, invalid cv count={}",
                            workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                }
            }
        } else {
            for (var segmentIndex : needMergeSegmentIndexList) {
                var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                if (hasValidCvSegmentIndexSet.contains(segmentIndex)) {
                    oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGED, mergeWorker.mergeWorkerId, snowFlake.nextId());

                    if (doLog) {
                        mergeWorker.log.info("Set segment flag to merged, w={}, s={}, b={}, i={}, mw={}, valid cv count={}, invalid cv count={}",
                                workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                    }

                    // add to merged set
                    mergeWorker.mergedSegmentSets[batchIndex].add(new ChunkMergeWorker.MergedSegment(workerId, slot, batchIndex, segmentIndex, validCvCountRecord.validCvCount));
                } else {
                    oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, mergeWorker.mergeWorkerId, 0L);

                    if (doLog) {
                        mergeWorker.log.info("Set segment flag to persisted, w={}, s={}, b={}, i={}, mw={}, valid cv count={}, invalid cv count={}",
                                workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                    }
                }
            }
        }

        var costT = System.nanoTime() - beginT;
        mergeWorker.mergedSegmentCostTotalTimeNanos += costT;

        mergeWorker.validCvCountTotal += validCvCountAfterRun;
        mergeWorker.invalidCvCountTotal += invalidCvCountAfterRun;
    }

    private void removeOld(OneSlot oneSlot, ArrayList<CvWithKeyAndSegmentOffset> cvList, ArrayList<ValidCvCountRecord> validCvCountRecordList) {
        var dictMap = DictMap.getInstance();
        var firstSegmentIndex = validCvCountRecordList.get(0).segmentIndex;
        ArrayList<CvWithKeyAndSegmentOffset> toRemoveCvList = new ArrayList<>(cvList.size());

        for (var cv : cvList) {
            cv.bucketIndex = localPersist.bucketIndex(cv.cv.getKeyHash());
            cv.walGroupIndex = Wal.calWalGroupIndex(cv.bucketIndex);
        }

        var groupByWalGroupIndex = cvList.stream().collect(Collectors.groupingBy(one -> one.walGroupIndex));
        for (var entry : groupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var keyBucketsInOneWalGroup = new KeyBucketsInOneWalGroup(slot, walGroupIndex, oneSlot.keyLoader);
            keyBucketsInOneWalGroup.readBeforePutBatch();

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
                        throw new IllegalStateException("Merge compressed value is newer than write batch compressed value, w=" + workerId +
                                ", s=" + slot + ", i=" + segmentIndex + ", key=" + key + ", merge cv seq=" + cv.getSeq() +
                                ", write batch cv seq=" + valueSeqCurrent);
                    }
                } else {
                    // compare is worker id, slot, offset is same
                    var pvmCurrent = PersistValueMeta.decode(valueBytesCurrent);
                    if (pvmCurrent.workerId != workerId || pvmCurrent.batchIndex != batchIndex
                            || pvmCurrent.segmentIndex != segmentIndex || pvmCurrent.subBlockIndex != subBlockIndex || pvmCurrent.segmentOffset != segmentOffset) {
                        // cv is old， discard
                        validCvCountRecord.invalidCvCount++;
                        toRemoveCvList.add(one);
                    } else {
                        if (cv.isExpired()) {
                            validCvCountRecord.invalidCvCount++;
                            toRemoveCvList.add(one);
                            // need not clear pvm, because key loader will clear
//                                oneSlot.removeDelay(mergeWorker.mergeWorkerId, key, bucketIndex, cv.getKeyHash(), cv.getSeq());

                            if (cv.isBigString()) {
                                // need remove file
                                var buffer = ByteBuffer.wrap(cv.getCompressedData());
                                var uuid = buffer.getLong();

                                var isDeleted = oneSlot.getBigStringFiles().deleteBigStringFileIfExist(uuid);
                                if (!isDeleted) {
                                    throw new RuntimeException("Delete big string file error, w=" + workerId + ", s=" + slot +
                                            ", b=" + batchIndex + ", i=" + segmentIndex + ", mw=" + mergeWorker.mergeWorkerId +
                                            ", key=" + key + ", uuid=" + uuid);
                                } else {
                                    mergeWorker.log.warn("Delete big string file, w={}, s={}, b={}, i={}, mw={}, key={}, uuid={}",
                                            workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, key, uuid);
                                }
                            }
                        } else {
                            validCvCountRecord.validCvCount++;

                            // if there is a new dict, compress use new dict and replace
                            if (cv.isUseDict() && cv.getDictSeqOrSpType() == Dict.SELF_ZSTD_DICT_SEQ) {
                                var dict = dictMap.getDict(TrainSampleJob.keyPrefix(key));
                                if (dict != null) {
                                    var rawBytes = cv.decompress(Dict.SELF_ZSTD_DICT);
                                    var newCompressedCv = CompressedValue.compress(rawBytes, dict, mergeWorker.compressLevel);
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
