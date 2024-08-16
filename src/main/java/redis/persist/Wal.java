package redis.persist;

import org.apache.lucene.util.RamUsageEstimator;
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
                    String key, byte[] cvEncoded, boolean isFromMerge) {
        boolean isRemove() {
            return CompressedValue.isDeleted(cvEncoded);
        }

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
            var n = is.read(keyBytes);
            if (n != keyLength) {
                throw new IllegalStateException("Read key bytes error, key length: " + keyLength + ", read length: " + n);
            }
            var cvEncodedLength = is.readInt();
            var cvEncoded = new byte[cvEncodedLength];
            var n2 = is.read(cvEncoded);
            if (n2 != cvEncodedLength) {
                throw new IllegalStateException("Read cv encoded bytes error, cv encoded length: " + cvEncodedLength + ", read length: " + n2);
            }

            if (vLength != ENCODED_HEADER_LENGTH + keyLength + cvEncodedLength) {
                throw new IllegalStateException("Invalid length: " + vLength);
            }

            return new V(seq, bucketIndex, keyHash, expireAt, new String(keyBytes), cvEncoded, false);
        }
    }

    record PutResult(boolean needPersist, boolean isValueShort, V needPutV, int offset) {
    }

    long initMemoryN = 0;

    public Wal(byte slot, int groupIndex, RandomAccessFile walSharedFile, RandomAccessFile walSharedFileShortValue,
               SnowFlake snowFlake) throws IOException {
        this.slot = slot;
        this.groupIndex = groupIndex;
        this.walSharedFile = walSharedFile;
        this.walSharedFileShortValue = walSharedFileShortValue;
        this.snowFlake = snowFlake;

        this.delayToKeyBucketValues = new HashMap<>();
        this.delayToKeyBucketShortValues = new HashMap<>();

        if (!ConfForSlot.global.pureMemory) {
            var n1 = readWal(walSharedFile, delayToKeyBucketValues, false);
            var n2 = readWal(walSharedFileShortValue, delayToKeyBucketShortValues, true);
            initMemoryN += RamUsageEstimator.shallowSizeOf(delayToKeyBucketValues);
            initMemoryN += RamUsageEstimator.shallowSizeOf(delayToKeyBucketShortValues);

            // reduce log
            if (slot == 0 && groupIndex == 0) {
                log.info("Read wal file success, slot: {}, group index: {}, value size: {}, short value size: {}, init memory n: {}KB",
                        slot, groupIndex, n1, n2, initMemoryN / 1024);
            }
        }
    }

    @Override
    public String toString() {
        return "Wal{" +
                "slot=" + slot +
                ", groupIndex=" + groupIndex +
                ", value.size=" + delayToKeyBucketValues.size() +
                ", shortValue.size=" + delayToKeyBucketShortValues.size() +
                '}';
    }

    // each bucket group use 64K in shared file
    // chunk batch write 4 segments, each segment has 3 or 4 sub blocks, each blocks may contain 10-40 keys, 64K is enough for 1 batch about 100-200 keys
    // the smaller, latency will be better, MAX_WAL_GROUP_NUMBER will be larger, wal memory will be larger
    public static int ONE_GROUP_BUFFER_SIZE = PAGE_SIZE * 16;
    // readonly
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
    int writePosition;
    int writePositionShortValue;

    private final byte slot;
    final int groupIndex;

    static int calWalGroupIndex(int bucketIndex) {
        return bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
    }

    public static int calcWalGroupNumber() {
        return ConfForSlot.global.confBucket.bucketsPerSlot / ConfForSlot.global.confWal.oneChargeBucketNumber;
    }

    private final RandomAccessFile walSharedFile;
    private final RandomAccessFile walSharedFileShortValue;
    private final SnowFlake snowFlake;

    HashMap<String, V> delayToKeyBucketValues;
    HashMap<String, V> delayToKeyBucketShortValues;

    public int getKeyCount() {
        return delayToKeyBucketValues.size() + delayToKeyBucketShortValues.size();
    }

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

        return readBytesToList(toMap, isShortValue, bufferBytes, 0, ONE_GROUP_BUFFER_SIZE);
    }

    private int readBytesToList(HashMap<String, V> toMap, boolean isShortValue, byte[] bufferBytes, int offset, int length) throws IOException {
        int n = 0;
        int position = 0;
        var is = new DataInputStream(new ByteArrayInputStream(bufferBytes, offset, length));
        while (true) {
            var v = V.decode(is);
            if (v == null) {
                break;
            }

            toMap.put(v.key, v);
            position += v.encodeLength();
            n++;
        }

        if (isShortValue) {
            writePositionShortValue = position;
        } else {
            writePosition = position;
        }
        return n;
    }

    private void resetWal(boolean isShortValue) {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

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
        if (isShortValue) {
            writePositionShortValue = 0;
        } else {
            writePosition = 0;
        }
    }

    // not thread safe
    void clear() {
        delayToKeyBucketValues.clear();
        delayToKeyBucketShortValues.clear();

        resetWal(false);
        resetWal(true);

        if (groupIndex % 100 == 0) {
            log.info("Clear wal, slot: {}, group index: {}", slot, groupIndex);
        }
    }

    long clearShortValuesCount = 0;
    long clearValuesCount = 0;

    public void clearShortValues() {
        delayToKeyBucketShortValues.clear();
        resetWal(true);

        clearShortValuesCount++;
        if (clearShortValuesCount % 1000 == 0) {
            log.info("Clear short values, slot: {}, group index: {}, count: {}", slot, groupIndex, clearShortValuesCount);
        }
    }

    public void clearValues() {
        delayToKeyBucketValues.clear();
        resetWal(false);

        clearValuesCount++;
        if (clearValuesCount % 1000 == 0) {
            log.info("Clear values, slot: {}, group index: {}, count: {}", slot, groupIndex, clearValuesCount);
        }
    }

    byte[] get(String key) {
        var vShort = delayToKeyBucketShortValues.get(key);
        var v = delayToKeyBucketValues.get(key);
        if (vShort == null && v == null) {
            return null;
        }

        if (vShort != null) {
            if (v == null) {
                return vShort.cvEncoded;
            } else {
                if (vShort.seq > v.seq) {
                    return vShort.cvEncoded;
                } else {
                    return v.cvEncoded;
                }
            }
        }

        return v.cvEncoded;
    }

    PutResult removeDelay(String key, int bucketIndex, long keyHash) {
        byte[] encoded = {CompressedValue.SP_FLAG_DELETE_TMP};
        var v = new V(snowFlake.nextId(), bucketIndex, keyHash, EXPIRE_NOW, key, encoded, false);

        return put(true, key, v);
    }

    boolean exists(String key) {
        var vShort = delayToKeyBucketShortValues.get(key);
        if (vShort != null) {
            // already removed
            return !vShort.isRemove();
        } else {
            return delayToKeyBucketValues.get(key) != null;
        }
    }

    // stats
    long needPersistCountTotal = 0;
    long needPersistKvCountTotal = 0;
    long needPersistOffsetTotal = 0;

    // slave catch up master binlog, replay wal, need update write position, be careful
    public void putFromX(V v, boolean isValueShort, int offset) {
        if (!ConfForSlot.global.pureMemory) {
            var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
            putVToFile(v, isValueShort, offset, targetGroupBeginOffset);

            // update write position
            var encodeLength = v.encodeLength();
            if (isValueShort) {
                writePositionShortValue = offset + encodeLength;
            } else {
                writePosition = offset + encodeLength;
            }
        }

        if (isValueShort) {
            delayToKeyBucketShortValues.put(v.key, v);
            delayToKeyBucketValues.remove(v.key);
        } else {
            delayToKeyBucketValues.put(v.key, v);
            delayToKeyBucketShortValues.remove(v.key);
        }
    }

    private void putVToFile(V v, boolean isValueShort, int offset, int targetGroupBeginOffset) {
        var raf = isValueShort ? walSharedFileShortValue : walSharedFile;
        try {
            raf.seek(targetGroupBeginOffset + offset);
            raf.write(v.encode());
        } catch (IOException e) {
            log.error("Write to file error", e);
            throw new RuntimeException("Write to file error: " + e.getMessage());
        }
    }

    // return need persist
    PutResult put(boolean isValueShort, String key, V v) {
        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;
        var offset = isValueShort ? writePositionShortValue : writePosition;

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
            putVToFile(v, isValueShort, offset, targetGroupBeginOffset);
        }
        if (isValueShort) {
            writePositionShortValue += encodeLength;
        } else {
            writePosition += encodeLength;
        }

        if (isValueShort) {
            delayToKeyBucketShortValues.put(key, v);
            delayToKeyBucketValues.remove(key);

            boolean needPersist = delayToKeyBucketShortValues.size() >= ConfForSlot.global.confWal.shortValueSizeTrigger;
            if (needPersist) {
                needPersistCountTotal++;
                needPersistKvCountTotal += delayToKeyBucketShortValues.size();
                needPersistOffsetTotal += writePositionShortValue;
            }
            return new PutResult(needPersist, true, null, needPersist ? 0 : offset);
        }

        delayToKeyBucketValues.put(key, v);
        delayToKeyBucketShortValues.remove(key);

        boolean needPersist = delayToKeyBucketValues.size() >= ConfForSlot.global.confWal.valueSizeTrigger;
        if (needPersist) {
            needPersistCountTotal++;
            needPersistKvCountTotal += delayToKeyBucketValues.size();
            needPersistOffsetTotal += writePosition;
        }
        return new PutResult(needPersist, false, null, needPersist ? 0 : offset);
    }

    public byte[] toSlaveExistsOneWalGroupBytes() throws IOException {
        // encoded length
        // 4 bytes for group index
        // 4 bytes for one group buffer size
        // 4 bytes for write position, both value and short
        // value encoded + short value encoded
        int n = 4 + 4 + 4 * 2 + ONE_GROUP_BUFFER_SIZE * 2;

        var bytes = new byte[n];
        var buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(groupIndex);
        buffer.putInt(ONE_GROUP_BUFFER_SIZE);
        buffer.putInt(writePosition);
        buffer.putInt(writePositionShortValue);

        var targetGroupBeginOffset = ONE_GROUP_BUFFER_SIZE * groupIndex;

        walSharedFile.seek(targetGroupBeginOffset);
        walSharedFile.read(bytes, 16, ONE_GROUP_BUFFER_SIZE);

        walSharedFileShortValue.seek(targetGroupBeginOffset);
        walSharedFileShortValue.read(bytes, 16 + ONE_GROUP_BUFFER_SIZE, ONE_GROUP_BUFFER_SIZE);

        return bytes;
    }

    public void fromMasterExistsOneWalGroupBytes(byte[] bytes) throws IOException {
        var buffer = ByteBuffer.wrap(bytes);
        var groupIndex1 = buffer.getInt();
        var oneGroupBufferSize = buffer.getInt();

        if (groupIndex1 != groupIndex) {
            throw new IllegalStateException("Repl slave fetch wal group error, slot: " + slot +
                    ", group index: " + groupIndex1 + ", expect group index: " + groupIndex);
        }
        if (oneGroupBufferSize != ONE_GROUP_BUFFER_SIZE) {
            throw new IllegalStateException("Repl slave fetch wal group error, slot: " + slot +
                    ", group index: " + groupIndex1 + ", one group buffer size: " + oneGroupBufferSize + ", expect size: " + ONE_GROUP_BUFFER_SIZE);
        }

        writePosition = buffer.getInt();
        writePositionShortValue = buffer.getInt();

        var targetGroupBeginOffset = oneGroupBufferSize * groupIndex1;

        walSharedFile.seek(targetGroupBeginOffset);
        walSharedFile.write(bytes, 16, oneGroupBufferSize);

        walSharedFileShortValue.seek(targetGroupBeginOffset);
        walSharedFileShortValue.write(bytes, 16 + oneGroupBufferSize, oneGroupBufferSize);

        delayToKeyBucketValues.clear();
        delayToKeyBucketShortValues.clear();
        var n1 = readBytesToList(delayToKeyBucketValues, false, bytes, 16, oneGroupBufferSize);
        var n2 = readBytesToList(delayToKeyBucketShortValues, true, bytes, 16 + oneGroupBufferSize, oneGroupBufferSize);
        log.warn("Repl slave fetch wal group success, slot: {}, group index: {}, value size: {}, short value size: {}",
                slot, groupIndex1, n1, n2);
    }
}
