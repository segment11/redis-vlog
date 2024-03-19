package redis.repl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.Dict;
import redis.persist.Wal;

import java.util.ArrayList;

public class NoopMasterUpdateCallback implements MasterUpdateCallback {
    private final Logger log = LoggerFactory.getLogger(NoopMasterUpdateCallback.class);

    @Override
    public void onKeyBucketUpdate(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, long seq, byte[] bytes) {
        log.warn("onKeyBucketUpdate called with slot: {}, bucketIndex: {}, splitIndex: {}, splitNumber: {}, seq: {}, bytes.length: {}",
                slot, bucketIndex, splitIndex, splitNumber, seq, bytes.length);
    }

    @Override
    public void onWalAppend(byte slot, int bucketIndex, byte batchIndex, boolean isValueShort, Wal.V v, int offset) {
        log.warn("onWalAppend called with slot: {}, bucketIndex: {}, batchIndex: {}, isValueShort: {}, offset: {}, v: {}",
                slot, bucketIndex, batchIndex, isValueShort, offset, v);
    }

    @Override
    public void onDictCreate(String key, Dict dict) {
        log.warn("onDictCreate called with key: {}, dict: {}", key, dict);
    }

    @Override
    public void onSegmentWrite(byte workerId, byte batchIndex, byte slot, int segmentLength,
                               int segmentIndex, int segmentCount, ArrayList<Long> segmentSeqList, byte[] bytes, int capacity) {
        log.warn("onSegmentWrite called with workerId: {}, batchIndex: {}, slot: {}, segmentLength: {}, segmentIndex: {}, segmentCount: {}, segmentSeqList: {}, bytes.length: {}, capacity: {}",
                workerId, batchIndex, slot, segmentLength, segmentIndex, segmentCount, segmentSeqList, bytes.length, capacity);
    }

    @Override
    public void onBigStringFileWrite(byte slot, long uuid, byte[] bytes) {
        log.warn("onBigStringFileWrite called with slot: {}, uuid: {}, bytes.length: {}", slot, uuid, bytes.length);
    }
}
