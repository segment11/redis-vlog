package redis.repl;

import redis.Dict;
import redis.persist.Wal;

public interface MasterUpdateCallback {
    // offset == 0, need clear values
    void onWalAppend(byte slot, int bucketIndex, boolean isValueShort, Wal.V v, int offset);

    boolean isToSlaveWalAppendBatchEmpty();

    void flushToSlaveWalAppendBatch();

    void onDictCreate(String key, Dict dict);

    void onBigStringFileWrite(byte slot, long uuid, byte[] bytes);
}
