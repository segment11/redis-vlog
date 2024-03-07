package redis.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.ConfForSlot;
import redis.SnowFlake;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static redis.CompressedValue.EXPIRE_NOW;

public class Wal implements OfStats {
    public record V(byte workerId, long seq, int bucketIndex, long keyHash, long expireAt, String key,
                    byte[] cvEncoded, int cvEncodedLength) implements Serializable {
        @Override
        public String toString() {
            return "V{" +
                    "workerId=" + workerId +
                    ", seq=" + seq +
                    ", bucketIndex=" + bucketIndex +
                    ", key='" + key + '\'' +
                    ", expireAt=" + expireAt +
                    ", cvEncoded.length=" + cvEncoded.length +
                    '}';
        }

        public boolean isDeleteFlag() {
            return cvEncoded.length == 1 && cvEncoded[0] == CompressedValue.SP_FLAG_DELETE_TMP;
        }

        public int persistLength() {
            // include key
            // 2 -> key length short
            return 2 + key.length() + cvEncoded.length;
        }

        // worker id + seq + bucket index + key hash + expire at long + key length + cv encoded length
        private static final int ENCODED_HEADER_LENGTH = 1 + 8 + 4 + 8 + 8 + 1 + 2;

        int encodeLength() {
            int vLength = ENCODED_HEADER_LENGTH + key.length() + cvEncoded.length;
            return 4 + vLength;
        }

        public byte[] encode() {
            int vLength = ENCODED_HEADER_LENGTH + key.length() + cvEncoded.length;
            // 4 -> vLength
            var bytes = new byte[4 + vLength];
            var buffer = ByteBuffer.wrap(bytes);

            buffer.putInt(vLength);
            buffer.put(workerId);
            buffer.putLong(seq);
            buffer.putInt(bucketIndex);
            buffer.putLong(keyHash);
            buffer.putLong(expireAt);
            buffer.put((byte) key.length());
            buffer.put(key.getBytes());
            buffer.putShort((short) cvEncoded.length);
            buffer.put(cvEncoded);
            return bytes;
        }

        public static V decode(DataInputStream is) throws IOException {
            if (is.available() < 4) {
                return null;
            }

            var vLength = is.readInt();
            if (vLength == 0) {
                return null;
            }

            var workerId = is.readByte();
            var seq = is.readLong();
            var bucketIndex = is.readInt();
            var keyHash = is.readLong();
            var expireAt = is.readLong();
            var keyLength = is.readByte();
            var keyBytes = new byte[keyLength];
            is.read(keyBytes);
            var cvEncodedLength = is.readShort();
            var cvEncoded = new byte[cvEncodedLength];
            is.read(cvEncoded);

            if (vLength != ENCODED_HEADER_LENGTH + keyLength + cvEncodedLength) {
                throw new IllegalStateException("Invalid length: " + vLength);
            }

            return new V(workerId, seq, bucketIndex, keyHash, expireAt, new String(keyBytes), cvEncoded, cvEncodedLength);
        }
    }

    public record PutResult(boolean needPersist, boolean isValueShort, V needPutV) {
    }

    public Wal(byte slot, int groupIndex, byte batchIndex, RandomAccessFile walSharedFile, RandomAccessFile walSharedFileShortValue,
               SnowFlake snowFlake) throws IOException {
        this.slot = slot;
        this.groupIndex = groupIndex;
        this.batchIndex = batchIndex;
        this.walSharedFile = walSharedFile;
        this.walSharedFileShortValue = walSharedFileShortValue;
        this.snowFlake = snowFlake;

        this.delayToKeyBucketValues = new HashMap<>(ConfForSlot.global.confWal.valueSizeTrigger);
        this.delayToKeyBucketShortValues = new HashMap<>(ConfForSlot.global.confWal.shortValueSizeTrigger);

        var n1 = readWal(walSharedFile, delayToKeyBucketValues, false);
        var n2 = readWal(walSharedFileShortValue, delayToKeyBucketShortValues, true);

        // reduce log
        if (slot == 0 && groupIndex == 0 && batchIndex == 0) {
            log.info("Read wal file success, slot: {}, group index: {}, batch index: {}, value size: {}, short value size: {}",
                    slot, groupIndex, batchIndex, n1, n2);
        }
    }

    @Override
    public String toString() {
        return "Wal{" +
                "slot=" + slot +
                ", batchIndex=" + batchIndex +
                ", delayToKeyBucketValues=" + delayToKeyBucketValues.size() +
                ", delayToKeyBucketShortValues=" + delayToKeyBucketShortValues.size() +
                ", lastUsedTimeMillis=" + lastUsedTimeMillis +
                ", persistCount=" + persistCount +
                ", persistCostTimeMillis=" + persistCostTimeMillis +
                '}';
    }

    // each bucket group use 2M in shared file
    private static final int ONE_GROUP_SIZE = 1024 * 1024 * 2;
    // for prepend
    public static final byte[] M2 = new byte[ONE_GROUP_SIZE];
    // 1 file max 2GB, 2M * 1K = 2GB
    public static final int MAX_WAL_GROUP_NUMBER = 1024;

    // current wal group write position in target group of wal file
    private final int[] writePositionArray = new int[MAX_WAL_GROUP_NUMBER];
    private final int[] writePositionArrayShortValue = new int[MAX_WAL_GROUP_NUMBER];

    private final Logger log = LoggerFactory.getLogger(Wal.class);

    private final byte slot;
    private final int groupIndex;
    final byte batchIndex;
    private final RandomAccessFile walSharedFile;
    private final RandomAccessFile walSharedFileShortValue;
    private final SnowFlake snowFlake;

    HashMap<String, V> delayToKeyBucketValues;
    HashMap<String, V> delayToKeyBucketShortValues;

    int getKeyCount() {
        return delayToKeyBucketValues.size() + delayToKeyBucketShortValues.size();
    }

    long lastUsedTimeMillis;

    long persistCount;
    long persistCostTimeMillis;

    int readWal(RandomAccessFile fromWalFile, HashMap<String, V> toMap, boolean isShortValue) throws IOException {
        var targetGroupBeginOffset = ONE_GROUP_SIZE * groupIndex;

        var bufferBytes = new byte[ONE_GROUP_SIZE];
        fromWalFile.seek(targetGroupBeginOffset);
        int readN = fromWalFile.read(bufferBytes);
        if (readN == -1) {
            return 0;
        }

        int n = 0;
        int position = 0;
        var is = new DataInputStream(new ByteArrayInputStream(bufferBytes));
        while (true) {
            var v = V.decode(is);
            if (v == null) {
                break;
            }

            toMap.put(v.key, v);
            position += v.encodeLength();
            n++;
        }

        var positionArray = isShortValue ? writePositionArrayShortValue : writePositionArray;
        positionArray[groupIndex] = position;
        return n;
    }

    private void truncateWal(boolean isShortValue) {
        var targetGroupBeginOffset = ONE_GROUP_SIZE * groupIndex;
        var raf = isShortValue ? walSharedFileShortValue : walSharedFile;
        var positionArray = isShortValue ? writePositionArrayShortValue : writePositionArray;

        synchronized (raf) {
            try {
                raf.seek(targetGroupBeginOffset);
                raf.write(M2);

                // reset write position
                positionArray[groupIndex] = 0;
            } catch (IOException e) {
                log.error("Truncate wal group error", e);
            }
        }
    }

    // not thread safe
    void clear() {
        delayToKeyBucketValues.clear();
        delayToKeyBucketShortValues.clear();

        truncateWal(false);
        truncateWal(true);

        log.info("Clear wal, slot: {}, group index: {}, batch index: {}", slot, groupIndex, batchIndex);
    }

    long clearShortValuesCount = 0;
    long clearValuesCount = 0;

    void clearShortValues() {
        delayToKeyBucketShortValues.clear();
        truncateWal(true);

        clearShortValuesCount++;
        if (clearShortValuesCount % 1000 == 0) {
            log.info("Clear short values, slot: {}, group index: {}, batch index: {}, count: {}", slot, groupIndex, batchIndex, clearShortValuesCount);
        }
    }

    void clearValues() {
        delayToKeyBucketValues.clear();
        truncateWal(false);

        clearValuesCount++;
        if (clearValuesCount % 1000 == 0) {
            log.info("Clear values, slot: {}, group index: {}, batch index: {}, count: {}", slot, groupIndex, batchIndex, clearValuesCount);
        }
    }

    byte[] get(String key) {
        var v = delayToKeyBucketShortValues.get(key);
        if (v != null) {
            return v.cvEncoded;
        }

        v = delayToKeyBucketValues.get(key);
        if (v != null) {
            return v.cvEncoded;
        }
        return null;
    }

    PutResult removeDelay(byte workerId, String key, int bucketIndex, long keyHash, long seq) {
        byte[] encoded = {CompressedValue.SP_FLAG_DELETE_TMP};
        var v = new V(workerId, snowFlake.nextId(), bucketIndex, keyHash, EXPIRE_NOW, key, encoded, 1);

        var old = delayToKeyBucketShortValues.put(key, v);
        if (old != null && old.seq > seq) {
            // ABA
            delayToKeyBucketShortValues.put(key, old);
            return new PutResult(false, true, null);
        }
        return new PutResult(delayToKeyBucketShortValues.size() >= ConfForSlot.global.confWal.shortValueSizeTrigger, true, null);
    }

    boolean remove(String key) {
        boolean r = delayToKeyBucketShortValues.remove(key) != null;
        boolean r2 = delayToKeyBucketValues.remove(key) != null;

        return r || r2;
    }

    // return need persist
    PutResult put(boolean isValueShort, String key, V v) {
        var targetGroupBeginOffset = ONE_GROUP_SIZE * groupIndex;
        var raf = isValueShort ? walSharedFileShortValue : walSharedFile;
        var positionArray = isValueShort ? writePositionArrayShortValue : writePositionArray;

        var encodeLength = v.encodeLength();

        synchronized (raf) {
            try {
                var offset = positionArray[groupIndex];
                if (offset + encodeLength > ONE_GROUP_SIZE) {
                    return new PutResult(true, isValueShort, v);
                }

                raf.seek(targetGroupBeginOffset + offset);
                raf.write(v.encode());
                positionArray[groupIndex] += encodeLength;
            } catch (IOException e) {
                log.error("Write to file error", e);
                throw new RuntimeException("Write to file error: " + e.getMessage());
            }
        }

        if (isValueShort) {
            delayToKeyBucketShortValues.put(key, v);
            delayToKeyBucketValues.remove(key);

            return new PutResult(delayToKeyBucketShortValues.size() >= ConfForSlot.global.confWal.shortValueSizeTrigger, true, null);
        }

        delayToKeyBucketValues.put(key, v);
        return new PutResult(delayToKeyBucketValues.size() >= ConfForSlot.global.confWal.valueSizeTrigger, false, null);
    }

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();
        final String prefix = "wal-s-" + slot + "-batch-" + batchIndex + " ";

        list.add(new StatKV(prefix + "delay list size", delayToKeyBucketValues.size()));
        list.add(new StatKV(prefix + "delay short list size", delayToKeyBucketShortValues.size()));
        return list;
    }
}
