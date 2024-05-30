package redis.repl;

import redis.Dict;
import redis.persist.Wal;
import redis.repl.content.ToSlaveBigStringFileWrite;
import redis.repl.content.ToSlaveDictCreate;
import redis.repl.content.ToSlaveWalAppendBatch;

public class SendToSlaveMasterUpdateCallback implements MasterUpdateCallback {
    private final GetCurrentSlaveReplPairList getCurrentSlaveReplPairList;
    private final ToSlaveWalAppendBatch toSlaveWalAppendBatch;

    public SendToSlaveMasterUpdateCallback(GetCurrentSlaveReplPairList getCurrentSlaveReplPairList, ToSlaveWalAppendBatch toSlaveWalAppendBatch) {
        this.getCurrentSlaveReplPairList = getCurrentSlaveReplPairList;
        this.toSlaveWalAppendBatch = toSlaveWalAppendBatch;
    }

    @Override
    public void onWalAppend(byte slot, int bucketIndex, boolean isValueShort, Wal.V v, int offset) {
        // todo, support async use eventloop
        var addBatchResult = toSlaveWalAppendBatch.addBatch(isValueShort, v, offset);
        if (addBatchResult.needSent()) {
            var replPairList = getCurrentSlaveReplPairList.get();
            for (var replPair : replPairList) {
                replPair.flushToSlaveWalAppendBatch(toSlaveWalAppendBatch);
            }

            // ignore some repl pair send fail, also reset, or it will be sent again and again
            // wait slave reconnect and sync from the beginning if send fail
            toSlaveWalAppendBatch.reset();
            if (!addBatchResult.isLastIncluded()) {
                toSlaveWalAppendBatch.addBatch(isValueShort, v, offset);
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
    public void onBigStringFileWrite(byte slot, long uuid, byte[] bytes) {
        var toSlaveBigStringFileWrite = new ToSlaveBigStringFileWrite(uuid, bytes);
        var replPairList = getCurrentSlaveReplPairList.get();
        for (var replPair : replPairList) {
            replPair.write(ReplType.big_string_file_write, toSlaveBigStringFileWrite);
        }
    }
}
