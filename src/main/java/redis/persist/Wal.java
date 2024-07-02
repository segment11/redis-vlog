package redis.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.ConfForSlot;
import redis.Debug;
import redis.SnowFlake;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static redis.CompressedValue.EXPIRE_NOW;
import static redis.persist.LocalPersist.PAGE_SIZE;

public class Wal {
    public record V(long seq, int bucketIndex, long keyHash, long expireAt,
                    String key, byte[] cvEncoded, int cvEncodedLength, boolean isFromMerge) {
        @Override
        public String toString() {
            return "V{" +
                    ", seq=" + seq +
                    ", bucketIndex=" + bucketIndex +
                    ", key='" + key + '\'' +
                    ", expireAt=" + expireAt +
                    ", cvEncoded.length=" + cvEncoded.length +
                    '}';
        }

        public int persistLength() {
            // include key
            return CompressedValue.KEY_HEADER_LENGTH + key.length() + cvEncoded.length;
        }

        public static int persistLength(int keyLength, int cvEncodedLength) {
            // include key
            return CompressedValue.KEY_HEADER_LENGTH + keyLength + cvEncodedLength;
        }

        // seq long + bucket index int + key hash long + expire at long +
        // key length short + cv encoded length int
        private static final int ENCODED_HEADER_LENGTH = 8 + 4 + 8 + 8 + 2 + 4;

        public int encodeLength() {
            int vLength = ENCODED_HEADER_LENGTH + key.length() + cvEncoded.length;
            return 4 + vLength;
        }

        public byte[] encode() {
            int vLength = ENCODED_HEADER_LENGTH + key.length() + cvEncoded.length;
            // 4 -> vLength
            var bytes = new byte[4 + vLength];
            var buffer = ByteBuffer.wrap(bytes);

            buffer.putInt(vLength);
            buffer.putLong(seq);
            buffer.putInt(bucketIndex);
            buffer.putLong(keyHash);
            buffer.putLong(expireAt);
            buffer.putShort((short) key.length());
            buffer.put(key.getBytes());
            buffer.putInt(cvEncoded.length);
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

            var seq = is.readLong();
            var bucketIndex = is.readInt();
            var keyHash = is.readLong();
            var expireAt = is.readLong();
            var keyLength = is.readShort();

            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length: " + keyLength);
            }

            var keyBytes = new byte[keyLength];
            is.read(keyBytes);
            var cvEncodedLength = is.readInt();
            var cvEncoded = new byte[cvEncodedLength];
            is.read(cvEncoded);

            if (vLength != ENCODED_HEADER_LENGTH + keyLength + cvEncodedLength) {
                throw new IllegalStateException("Invalid length: " + vLength);
            }

            return new V(seq, bucketIndex, keyHash, expireAt, new String(keyBytes), cvEncoded, cvEncodedLength, false);
        }
    }

    record PutResult(boolean needPersist, boolean isValueShort, V needPutV, int offset) {
    }

    public Wal(byte slot, int groupIndex, RandomAccessFile walSharedFile, RandomAccessFile walSharedFileShortValue,
               SnowFlake snowFlake) throws IOException {
        this.slot = slot;
        this.groupIndex = groupIndex;
        this.walSharedFile = walSharedFile;
        this.walSharedFileShortValue = walSharedFileShortValue;
        this.snowFlake = snowFlake;

        this.delayToKeyBucketValues = new HashMap<>(ConfForSlot.global.confWal.valueSizeTrigger);
        this.delayToKeyBucketShortValues = new HashMap<>(ConfForSlot.global.confWal.shortValueSizeTrigger);

        if (!ConfForSlot.global.pureMemory) {
            var n1 = readWal(walSharedFile, delayToKeyBucketValues, false);
            var n2 = readWal(walSharedFileShortValue, delayToKeyBucketShortValues, true);

            // reduce log
            if (slot == 0 && groupIndex == 0) {
                log.info("Read wal file success, slot: {}, group index: {}, value size: {}, short value size: {}",
                        slot, groupIndex, n1, n2);
            }
        }
    }

    @Override
    public String toString() {
        return "Wal{" +
                "slot=" + slot +
                ", delayToKeyBucketValues=" + delayToKeyBucketValues.size() +
                ", delayToKeyBucketShortValues=" + delayToKeyBucketShortValues.size() +
                ", persistCount=" + persistCount +
                ", persistCostTimeMillis=" + persistCostTimeMillis +
                '}';
    }

    // each bucket group use 64K in shared file
    // chunk batch write 4 segments, each segment has 3 or 4 sub blocks, each blocks may contain 10-40 keys, 64K is enough for 1 batch about 100-200 keys
    // the smaller, latency will be better, MAX_WAL_GROUP_NUMBER will be larger, wal memory will be larger
    public static int ONE_GROUP_BUFFER_SIZE = PAGE_SIZE * 16;
    public static byte[] EMPTY_BYTES_FOR_ONE_GROUP = new byte[ONE_GROUP_BUFFER_SIZE];
    // for init prepend wal file
    static final byte[] INIT_M4 = new byte[1024 * 1024 * 4];
    public static int GROUP_COUNT_IN_M4 = 1024 * 1024 * 4 / ONE_GROUP_BUFFER_SIZE;

    // for latency, do not configure too large
    public static final List<Integer> VALID_ONE_CHARGE_BUCKET_NUMBER_LIST = List.of(16, 32, 64);
    // 1 file max 2GB, 2GB / 64K = 32K wal groups
    public static final int MAX_WAL_GROUP_NUMBER = (int) (2L * 1024 * 1024 * 1024 / (PAGE_SIZE * VALID_ONE_CHARGE_BUCKET_NUMBER_LIST.getFirst()));

    private static final Logger log = LoggerFactory.getLogger(Wal.class);

    public static void doLogAfterInit() {
        log.warn("Init wal groups, one group buffer size: {}KB, one charge bucket number: {}, wal group number: {}",
                ONE_GROUP_BUFFER_SIZE / 1024, ConfForSlot.global.confWal.oneChargeBucketNumber, calcWalGroupNumber());
    }

    // current wal group write position in target group of wal file
    private final int[] writePositionArray = new int[MAX_WAL_GROUP_NUMBER];
    private final int[] writePositionArrayShortValue = new int[MAX_WAL_GROUP_NUMBER];

    private final byte slot;
    final int groupIndex;

    static int calWalGroupIndex(int bucketIndex) {
        return bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
    }

    static int calcWalGroupNumber() {
        return ConfForSlot.global.confBucket.bucketsPerSlot / ConfForSlot.global.confWal.oneChargeBucketNumber;
    }

    private final RandomAccessFile walSharedFile;
    private final RandomAccessFile walSharedFileShortValue;
    private final SnowFlake snowFlake;

    HashMap<String, V> delayToKeyBucketValues;
    HashMap<String, V> delayToKeyBucketShortValues;

    int getKeyCount() {
        return delayToKeyBucketValues.size() + delayToKeyBucketShortValues.size();
    }

    long persistCount;
    long persistCostTimeMillis;

    int readWal(RandomAccessFile fromWalFile, HashMap<String, V> toMap, boolean isShortValue) throws IOException {
        // for unit test
        if (fromWalFile == null) {
            return 0;
        }

        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

        var bufferBytes = new byte[ONE_GROUP_BUFFER_SIZE];
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
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
        var positionArray = isShortValue ? writePositionArrayShortValue : writePositionArray;

        if (!ConfForSlot.global.pureMemory) {
            var raf = isShortValue ? walSharedFileShortValue : walSharedFile;
            try {
                raf.seek(targetGroupBeginOffset);
                raf.write(EMPTY_BYTES_FOR_ONE_GROUP);
            } catch (IOException e) {
                log.error("Truncate wal group error", e);
            }
        }

        // reset write position
        positionArray[groupIndex] = 0;
    }

    // not thread safe
    void clear() {
        delayToKeyBucketValues.clear();
        delayToKeyBucketShortValues.clear();

        truncateWal(false);
        truncateWal(true);

        if (groupIndex % 100 == 0) {
            log.info("Clear wal, slot: {}, group index: {}", slot, groupIndex);
        }
    }

    long clearShortValuesCount = 0;
    long clearValuesCount = 0;

    void clearShortValues() {
        delayToKeyBucketShortValues.clear();
        truncateWal(true);

        clearShortValuesCount++;
        if (clearShortValuesCount % 1000 == 0) {
            log.info("Clear short values, slot: {}, group index: {}, count: {}", slot, groupIndex, clearShortValuesCount);
        }
    }

    void clearValues() {
        delayToKeyBucketValues.clear();
        truncateWal(false);

        clearValuesCount++;
        if (clearValuesCount % 1000 == 0) {
            log.info("Clear values, slot: {}, group index: {}, count: {}", slot, groupIndex, clearValuesCount);
        }
    }

    byte[] get(String key) {
        var v = delayToKeyBucketShortValues.get(key);
        var v2 = delayToKeyBucketValues.get(key);
        if (v == null && v2 == null) {
            return null;
        }

        if (v != null) {
            if (v2 == null) {
                return v.cvEncoded;
            } else {
                if (v.seq > v2.seq) {
                    return v.cvEncoded;
                } else {
                    return v2.cvEncoded;
                }
            }
        }

        if (v2 != null) {
            return v2.cvEncoded;
        }

        return null;
    }

    PutResult removeDelay(String key, int bucketIndex, long keyHash) {
        byte[] encoded = {CompressedValue.SP_FLAG_DELETE_TMP};
        var v = new V(snowFlake.nextId(), bucketIndex, keyHash, EXPIRE_NOW, key, encoded, 1, false);

        return put(true, key, v);
    }

    boolean remove(String key) {
        boolean r = delayToKeyBucketShortValues.remove(key) != null;
        boolean r2 = delayToKeyBucketValues.remove(key) != null;

        return r || r2;
    }

    void writeRafAndOffsetFromMasterNewly(boolean isValueShort, V v, int offset) {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
        var positionArray = isValueShort ? writePositionArrayShortValue : writePositionArray;
        var encodeLength = v.encodeLength();

        if (!ConfForSlot.global.pureMemory) {
            var raf = isValueShort ? walSharedFileShortValue : walSharedFile;
            try {
                raf.seek(targetGroupBeginOffset + offset);
                raf.write(v.encode());
            } catch (IOException e) {
                log.error("Write to file error", e);
                throw new RuntimeException("Write to file error: " + e.getMessage());
            }
        }

        positionArray[groupIndex] += encodeLength;
    }

    long needPersistCountTotal = 0;
    long needPersistKvCountTotal = 0;
    long needPersistOffsetTotal = 0;

    // return need persist
    PutResult put(boolean isValueShort, String key, V v) {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
        var positionArray = isValueShort ? writePositionArrayShortValue : writePositionArray;
        int offset = positionArray[groupIndex];

        var encodeLength = v.encodeLength();
        if (offset + encodeLength > ONE_GROUP_BUFFER_SIZE) {
            needPersistCountTotal++;
            var keyCount = isValueShort ? delayToKeyBucketShortValues.size() : delayToKeyBucketValues.size();
            needPersistKvCountTotal += keyCount;
            needPersistOffsetTotal += offset;

            return new PutResult(true, isValueShort, v, 0);
        }

        var bulkLoad = Debug.getInstance().bulkLoad;
        // bulk load need not wal write
        if (!ConfForSlot.global.pureMemory && !bulkLoad) {
            var raf = isValueShort ? walSharedFileShortValue : walSharedFile;
            try {
                raf.seek(targetGroupBeginOffset + offset);
                raf.write(v.encode());
            } catch (IOException e) {
                log.error("Write to file error", e);
                throw new RuntimeException("Write to file error: " + e.getMessage());
            }
        }
        positionArray[groupIndex] += encodeLength;

        if (isValueShort) {
            delayToKeyBucketShortValues.put(key, v);
            delayToKeyBucketValues.remove(key);

            boolean needPersist = delayToKeyBucketShortValues.size() >= ConfForSlot.global.confWal.shortValueSizeTrigger;
            if (needPersist) {
                needPersistCountTotal++;
                needPersistKvCountTotal += delayToKeyBucketShortValues.size();
                needPersistOffsetTotal += positionArray[groupIndex];
            }
            return new PutResult(needPersist, true, null, needPersist ? 0 : offset);
        }

        delayToKeyBucketValues.put(key, v);
        delayToKeyBucketShortValues.remove(key);

        boolean needPersist = delayToKeyBucketValues.size() >= ConfForSlot.global.confWal.valueSizeTrigger;
        if (needPersist) {
            needPersistCountTotal++;
            needPersistKvCountTotal += delayToKeyBucketValues.size();
            needPersistOffsetTotal += positionArray[groupIndex];
        }
        return new PutResult(needPersist, false, null, needPersist ? 0 : offset);
    }
}
