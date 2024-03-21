package redis.repl;

import redis.Dict;
import redis.persist.Wal;

import java.util.ArrayList;

public interface MasterUpdateCallback {
    void onKeyBucketUpdate(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, long seq, byte[] bytes);

    // offset == 0, need clear values
    void onWalAppend(byte slot, int bucketIndex, byte batchIndex, boolean isValueShort, Wal.V v, int offset);

    boolean isToSlaveWalAppendBatchEmpty();

    void flushToSlaveWalAppendBatch();

    void onDictCreate(String key, Dict dict);

    void onSegmentWrite(byte workerId, byte batchIndex, byte slot, int segmentLength,
                        int segmentIndex, int segmentCount, ArrayList<Long> segmentSeqList, byte[] bytes, int capacity);

    void onBigStringFileWrite(byte slot, long uuid, byte[] bytes);
}
