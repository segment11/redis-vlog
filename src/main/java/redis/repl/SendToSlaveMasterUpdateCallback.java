package redis.repl;

import redis.Dict;
import redis.persist.Wal;
import redis.repl.content.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SendToSlaveMasterUpdateCallback implements MasterUpdateCallback {
    private final GetCurrentSlaveReplPairList getCurrentSlaveReplPairList;
    private final ToSlaveWalAppendBatch toSlaveWalAppendBatch;

    public SendToSlaveMasterUpdateCallback(GetCurrentSlaveReplPairList getCurrentSlaveReplPairList, ToSlaveWalAppendBatch toSlaveWalAppendBatch) {
        this.getCurrentSlaveReplPairList = getCurrentSlaveReplPairList;
        this.toSlaveWalAppendBatch = toSlaveWalAppendBatch;
    }

    @Override
    public void onKeyBucketUpdate(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, long lastUpdateSeq, byte[] bytes) {
        var toSlaveKeyBucketUpdate = new ToSlaveKeyBucketUpdate(bucketIndex, splitIndex, splitNumber, lastUpdateSeq, bytes);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.key_bucket_update, toSlaveKeyBucketUpdate);
        }
    }

    @Override
    public void onKeyBucketSplit(byte slot, int bucketIndex, byte splitNumber) {
        var toSlaveKeyBucketSplit = new ToSlaveKeyBucketSplit(bucketIndex, splitNumber);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.key_bucket_split, toSlaveKeyBucketSplit);
        }
    }

    @Override
    public void onWalAppend(byte slot, int bucketIndex, byte batchIndex, boolean isValueShort, Wal.V v, int offset) {
        var addBatchResult = toSlaveWalAppendBatch.addBatch(batchIndex, isValueShort, v, offset);
        if (addBatchResult.needSent()) {
            var replPairList = getCurrentSlaveReplPairList.get();
            for (var replPair : replPairList) {
                replPair.flushToSlaveWalAppendBatch(toSlaveWalAppendBatch);
            }

            // ignore some repl pair send fail, also reset, or it will be sent again and again
            // wait slave reconnect and sync from the beginning if send fail
            toSlaveWalAppendBatch.reset();
            if (!addBatchResult.isLastIncluded()) {
                toSlaveWalAppendBatch.addBatch(batchIndex, isValueShort, v, offset);
            }
        }
    }

    @Override
    public boolean isToSlaveWalAppendBatchEmpty() {
        return toSlaveWalAppendBatch.isEmpty();
    }

    @Override
    public void flushToSlaveWalAppendBatch() {
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.flushToSlaveWalAppendBatch(toSlaveWalAppendBatch);
        }
        toSlaveWalAppendBatch.reset();
    }

    @Override
    public void onDictCreate(String key, Dict dict) {
        var toSlaveDictCreate = new ToSlaveDictCreate(key, dict);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.dict_create, toSlaveDictCreate);
        }
    }

    @Override
    public void onSegmentWrite(byte workerId, byte batchIndex, byte slot, int segmentLength, int segmentIndex, int segmentCount,
                               ArrayList<Long> segmentSeqList, byte[] bytes, int capacity) {
        var toSlaveSegmentWrite = new ToSlaveSegmentWrite(workerId, batchIndex, segmentLength, segmentIndex, segmentCount,
                segmentSeqList, bytes, capacity);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.segment_write, toSlaveSegmentWrite);
        }
    }

    @Override
    public void onBigStringFileWrite(byte slot, long uuid, byte[] bytes) {
        var toSlaveBigStringFileWrite = new ToSlaveBigStringFileWrite(uuid, bytes);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.big_string_file_write, toSlaveBigStringFileWrite);
        }
    }

    @Override
    public void onSegmentIndexChange(byte workerId, byte batchIndex, int segmentIndex) {
        var bytes = new byte[1 + 1 + 4];
        bytes[0] = workerId;
        bytes[1] = batchIndex;
        ByteBuffer.wrap(bytes, 2, 4).putInt(segmentIndex);

        var replContent = new RawBytesContent(bytes);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.segment_index_change, replContent);
        }
    }
}
