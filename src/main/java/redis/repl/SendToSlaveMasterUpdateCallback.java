package redis.repl;

import redis.Dict;
import redis.persist.Wal;
import redis.repl.content.ToSlaveWalAppendBatch;

import java.util.ArrayList;

public class SendToSlaveMasterUpdateCallback implements MasterUpdateCallback {
    private final GetCurrentSlaveReplPairList getCurrentSlaveReplPairList;
    private final ToSlaveWalAppendBatch toSlaveWalAppendBatch;

    public SendToSlaveMasterUpdateCallback(GetCurrentSlaveReplPairList getCurrentSlaveReplPairList, ToSlaveWalAppendBatch toSlaveWalAppendBatch) {
        this.getCurrentSlaveReplPairList = getCurrentSlaveReplPairList;
        this.toSlaveWalAppendBatch = toSlaveWalAppendBatch;
    }

    @Override
    public void onKeyBucketUpdate(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, long seq, byte[] bytes) {

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

    }

    @Override
    public void onSegmentWrite(byte workerId, byte batchIndex, byte slot, int segmentLength, int segmentIndex, int segmentCount, ArrayList<Long> segmentSeqList, byte[] bytes, int capacity) {

    }

    @Override
    public void onBigStringFileWrite(byte slot, long uuid, byte[] bytes) {

    }
}
