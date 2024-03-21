package redis.repl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.Dict;
import redis.persist.Wal;

import java.util.ArrayList;

// for debug and test
public class NoopMasterUpdateCallback implements MasterUpdateCallback {
    private final Logger log = LoggerFactory.getLogger(NoopMasterUpdateCallback.class);

    private long keyBucketUpdateCount = 0;

    @Override
    public void onKeyBucketUpdate(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, long seq, byte[] bytes) {
        keyBucketUpdateCount++;
        if (keyBucketUpdateCount % 10000 == 0) {
            log.warn("onKeyBucketUpdate called with slot: {}, bucketIndex: {}, splitIndex: {}, splitNumber: {}, seq: {}, bytes.length: {}",
                    slot, bucketIndex, splitIndex, splitNumber, seq, bytes.length);
        }
    }

    private long walAppendCount = 0;

    @Override
    public void onWalAppend(byte slot, int bucketIndex, byte batchIndex, boolean isValueShort, Wal.V v, int offset) {
        walAppendCount++;
        if (walAppendCount % 100000 == 0) {
            log.warn("onWalAppend called with slot: {}, bucketIndex: {}, batchIndex: {}, isValueShort: {}, offset: {}, v: {}",
                    slot, bucketIndex, batchIndex, isValueShort, offset, v);
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

    private long segmentWriteCount = 0;

    @Override
    public void onSegmentWrite(byte workerId, byte batchIndex, byte slot, int segmentLength,
                               int segmentIndex, int segmentCount, ArrayList<Long> segmentSeqList, byte[] bytes, int capacity) {
        segmentWriteCount++;
        if (segmentWriteCount % 1000 == 0) {
            log.warn("onSegmentWrite called with workerId: {}, batchIndex: {}, slot: {}, segmentLength: {}, segmentIndex: {}, segmentCount: {}, segmentSeqList: {}, bytes.length: {}, capacity: {}",
                    workerId, batchIndex, slot, segmentLength, segmentIndex, segmentCount, segmentSeqList, bytes.length, capacity);
        }
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
