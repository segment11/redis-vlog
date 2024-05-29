package redis.repl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.Dict;
import redis.persist.Wal;

// for debug and test
public class NoopMasterUpdateCallback implements MasterUpdateCallback {
    private final Logger log = LoggerFactory.getLogger(NoopMasterUpdateCallback.class);

    private long walAppendCount = 0;

    @Override
    public void onWalAppend(byte slot, int bucketIndex, boolean isValueShort, Wal.V v, int offset) {
        walAppendCount++;
        if (walAppendCount % 100000 == 0) {
            log.warn("onWalAppend called with slot: {}, bucketIndex: {}, isValueShort: {}, offset: {}, v: {}",
                    slot, bucketIndex, isValueShort, offset, v);
        }
    }

    @Override
    public boolean isToSlaveWalAppendBatchEmpty() {
        return false;
    }

    @Override
    public void flushToSlaveWalAppendBatch() {

    }

    @Override
    public void onDictCreate(String key, Dict dict) {
        log.warn("onDictCreate called with key: {}, dict: {}", key, dict);
    }

    private long bigStringFileWriteCount = 0;

    @Override
    public void onBigStringFileWrite(byte slot, long uuid, byte[] bytes) {
        bigStringFileWriteCount++;
        if (bigStringFileWriteCount % 100 == 0) {
            log.warn("onBigStringFileWrite called with slot: {}, uuid: {}, bytes.length: {}", slot, uuid, bytes.length);
        }
    }
}
