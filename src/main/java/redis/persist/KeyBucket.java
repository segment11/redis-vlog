package redis.persist;

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressStats;
import redis.SnowFlake;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.persist.KeyLoader.MAX_SPLIT_NUMBER;
import static redis.persist.KeyLoader.SPLIT_MULTI_STEP;
import static redis.persist.PersistValueMeta.ENCODED_LEN;

public class KeyBucket {
    private static final short INIT_CAPACITY = 60;
    // 16KB compress ratio better, 4KB decompress faster
    private static final int INIT_BYTES_LENGTH = LocalPersist.PAGE_SIZE;
    // if big, wal will cost too much memory
    public static final int MAX_BUCKETS_PER_SLOT = KeyLoader.KEY_BUCKET_COUNT_PER_FD;
    public static final int DEFAULT_BUCKETS_PER_SLOT = 16384;

    // key length short 2 + key length <= 32 + value length byte 1 + (pvm length 24 or short value case number 17 / string 19 ) < 60
    // if key length > 32, refer CompressedValue.KEY_MAX_LENGTH, one key may cost 2 cells
    // 8 + 60 * (60 + 8) = 4088, in 4K page size
    private static final int ONE_CELL_LENGTH = 60;
    private static final int HASH_VALUE_LENGTH = 8;
    private static final int SEQ_VALUE_LENGTH = 8;
    // seq long + size int + uncompressed length int + compressed length int
    static final int AFTER_COMPRESS_PREPEND_LENGTH = SEQ_VALUE_LENGTH + 4 + 4 + 4;

    static final double HIGH_LOAD_FACTOR = 0.8;
    static final double LOW_LOAD_FACTOR = 0.2;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int capacity;
    int size;
    int cellCost;

    long lastUpdateSeq;

    private final SnowFlake snowFlake;

    long lastSplitCostNanos;

    public double loadFactor() {
        return size / (double) capacity;
    }

    private int firstCellOffset() {
        // key masked value long 8 bytes for each cell
        return capacity * HASH_VALUE_LENGTH;
    }

    static final long NO_KEY = 0;
    static final long PRE_KEY = -1;

    private final byte slot;
    private final int bucketIndex;
    final byte splitIndex;
    byte splitNumber;

    @Override
    public String toString() {
        return "KeyBucket{" +
                "slot=" + slot +
                ", bucketIndex=" + bucketIndex +
                ", splitIndex=" + splitIndex +
                ", splitNumber=" + splitNumber +
                ", capacity=" + capacity +
                ", size=" + size +
                ", cellCost=" + cellCost +
                ", loadFactor=" + loadFactor() +
                ", lastUpdateSeq=" + lastUpdateSeq +
                '}';
    }

    // compressed
    private final byte[] compressedData;

    byte[] decompressBytes;

    CompressStats compressStats;

    public void initWithCompressStats(CompressStats compressStats) {
        this.compressStats = compressStats;
        this.init();
    }

    public KeyBucket(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, @Nullable byte[] compressedData, SnowFlake snowFlake) {
        this.slot = slot;
        this.bucketIndex = bucketIndex;
        this.splitIndex = splitIndex;
        this.splitNumber = splitNumber;
        this.compressedData = compressedData;
        this.capacity = INIT_CAPACITY;
        this.size = 0;
        this.cellCost = 0;
        this.snowFlake = snowFlake;
    }

    // for split
    private KeyBucket(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, int capacity, SnowFlake snowFlake) {
        this.slot = slot;
        this.bucketIndex = bucketIndex;
        this.splitIndex = splitIndex;
        this.splitNumber = splitNumber;
        this.compressedData = null;
        this.capacity = capacity;
        this.size = 0;
        this.cellCost = 0;
        this.snowFlake = snowFlake;

        this.decompressBytes = new byte[capacity * HASH_VALUE_LENGTH + capacity * ONE_CELL_LENGTH];
        this.buffer = ByteBuffer.wrap(decompressBytes);
    }

    interface IterateCallBack {
        void call(long cellHashValue, byte[] keyBytes, byte[] valueBytes);
    }

    void iterate(IterateCallBack callBack) {
        for (int i = 0; i < capacity; i++) {
            var cellHashValue = buffer.getLong(i * HASH_VALUE_LENGTH);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var cellOffset = firstCellOffset() + i * ONE_CELL_LENGTH;
            buffer.position(cellOffset);

            var keyLength = buffer.getShort();
            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.get();
            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            callBack.call(cellHashValue, keyBytes, valueBytes);
        }
    }

    private record KVMeta(int offset, short keyLength, byte valueLength) {
        int valueOffset() {
            return offset + Short.BYTES + keyLength + Byte.BYTES;
        }

        int cellCount() {
            int keyWithValueBytesLength = Short.BYTES + keyLength + Byte.BYTES + valueLength;
            int cellCount = keyWithValueBytesLength / ONE_CELL_LENGTH;
            if (keyWithValueBytesLength % ONE_CELL_LENGTH != 0) {
                cellCount++;
            }
            return cellCount;
        }

        @Override
        public String toString() {
            return "KVMeta{" +
                    "offset=" + offset +
                    ", keyLength=" + keyLength +
                    ", valueLength=" + valueLength +
                    '}';
        }
    }

    private ByteBuffer buffer;

    private void init() {
        if (buffer != null) {
            return;
        }

        if (compressedData == null || compressedData.length == 0) {
            decompressBytes = new byte[INIT_BYTES_LENGTH];
            size = 0;
        } else {
            var bufferInner = ByteBuffer.wrap(compressedData);
            // first 8 bytes is seq
            lastUpdateSeq = bufferInner.getLong();

            // then 4 bytes is size
            size = bufferInner.getInt();
            compressStats.updateTmpBucketSize(slot, bucketIndex, splitIndex, size);

            // then 4 bytes is uncompressed length
            int uncompressedLength = bufferInner.getInt();
            // fix this, Destination buffer is too small, todo
            decompressBytes = new byte[uncompressedLength];

            int compressedSize = bufferInner.getInt();

            long begin = System.nanoTime();
            Zstd.decompressByteArray(decompressBytes, 0, uncompressedLength,
                    compressedData, AFTER_COMPRESS_PREPEND_LENGTH, compressedSize);
            long costT = System.nanoTime() - begin;

            // stats
            // thread not safe, use long adder
            compressStats.decompressCount2.increment();
            compressStats.decompressCostTotalTimeNanos2.add(costT);

//            compressStats.rawValueBodyTotalLength2.add(uncompressedLength);
//            compressStats.compressedValueBodyTotalLength2.add(compressedData.length);
        }
        buffer = ByteBuffer.wrap(decompressBytes);
    }

    public byte[] compress() {
        var maxDstSize = (int) Zstd.compressBound(decompressBytes.length);
        var dst = new byte[maxDstSize + AFTER_COMPRESS_PREPEND_LENGTH];
        int compressedSize = (int) Zstd.compressByteArray(dst, AFTER_COMPRESS_PREPEND_LENGTH, maxDstSize,
                decompressBytes, 0, decompressBytes.length, Zstd.defaultCompressionLevel());

        int afterCompressPersistSize = compressedSize + AFTER_COMPRESS_PREPEND_LENGTH;
        if (afterCompressPersistSize > KeyLoader.KEY_BUCKET_ONE_COST_SIZE) {
            throw new IllegalStateException("Compressed size too large, compressed size=" + afterCompressPersistSize);
        }

        // put to cache use minimize size
        // dst include too many 0
        var r = Arrays.copyOfRange(dst, 0, afterCompressPersistSize);
        var bufferInner = ByteBuffer.wrap(r);
        bufferInner.putLong(lastUpdateSeq).putInt(size).putInt(decompressBytes.length).putInt(compressedSize);

        // stats
        compressStats.compressedValueSizeTotalCount2.add(size);
        compressStats.compressedValueBodyTotalLength2.add(compressedSize);

        compressStats.updateTmpBucketSize(slot, bucketIndex, splitIndex, size);
        return r;
    }

    int clearExpiredPvm() {
        int n = 0;
        // use new buffer as clearCell used exist buffer
        var bufferNew = ByteBuffer.wrap(decompressBytes);
        for (int i = 0; i < capacity; i++) {
            var cellHashValue = bufferNew.getLong(i * HASH_VALUE_LENGTH);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var cellOffset = firstCellOffset() + i * ONE_CELL_LENGTH;
            bufferNew.position(cellOffset);

            var keyLength = bufferNew.getShort();
            // skip key
            bufferNew.position(bufferNew.position() + keyLength);
            var valueLength = bufferNew.get();
            var kvMeta = new KVMeta(cellOffset, keyLength, valueLength);

            var valueFirstByte = bufferNew.get();
            var isPvm = valueFirstByte >= 0 && (valueLength == ENCODED_LEN);
            if (isPvm) {
                // last 8 bytes is expireAt
                // 1 value byte already read
                long expireAt = bufferNew.getLong(bufferNew.position() - 1 + valueLength - 8);
                if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                    clearCell(i, kvMeta.cellCount());
                    n++;
                }
            }
        }
        return n;
    }

    String allPrint() {
        var sb = new StringBuilder();
        iterate((keyHash, keyBytes, valueBytes) -> sb.append("key=").append(new String(keyBytes))
                .append(", value=").append(new String(valueBytes)).append("\n"));
        return sb.toString();
    }

    // because * 2 may be data skew
    KeyBucket[] split() {
        // bucket per slot usually % 8 == 0, % 3 is better for data skew
        var newSplitNumber = (byte) (splitNumber * SPLIT_MULTI_STEP);
        if (newSplitNumber > MAX_SPLIT_NUMBER) {
            throw new BucketFullException("Split number too large, new split number=" + newSplitNumber);
        }

        // calc meta and move cell
        long begin = System.nanoTime();

        var oldSplitNumber = splitNumber;
        this.splitNumber = newSplitNumber;

        var keyBuckets = new KeyBucket[SPLIT_MULTI_STEP];
        // self split index not change
        keyBuckets[0] = this;

        for (int i = 1; i < SPLIT_MULTI_STEP; i++) {
            // split others
            // split index change
            var splitKeyBucket = new KeyBucket(slot, bucketIndex, (byte) (splitIndex + i * oldSplitNumber),
                    newSplitNumber, this.capacity, this.snowFlake);
            splitKeyBucket.compressStats = compressStats;
            keyBuckets[i] = splitKeyBucket;
        }

        for (int i = 0; i < capacity; i++) {
            var cellHashValue = buffer.getLong(i * HASH_VALUE_LENGTH);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var cellOffset = firstCellOffset() + i * ONE_CELL_LENGTH;
            buffer.position(cellOffset);
            var keyLength = buffer.getShort();
            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.get();
            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            var kvMeta = new KVMeta(cellOffset, keyLength, valueLength);

            var valueFirstByte = valueBytes[0];
            var isPvm = valueFirstByte >= 0 && (valueLength == ENCODED_LEN);
            if (isPvm) {
                // last 8 bytes is expireAt
                long expireAt = buffer.getLong(buffer.position() - 8);
                if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                    var cellCount = kvMeta.cellCount();
                    clearCell(i, cellCount);
                    size--;
                    cellCost -= cellCount;
                    continue;
                }
            }

            int newSplitIndex = (int) Math.abs(cellHashValue % newSplitNumber);
            boolean isStillInCurrentSplit = newSplitIndex == splitIndex;
            if (isStillInCurrentSplit) {
                continue;
            }

            KeyBucket targetKeyBucket = null;
            for (var splitKeyBucket : keyBuckets) {
                if (newSplitIndex == splitKeyBucket.splitIndex) {
                    targetKeyBucket = splitKeyBucket;
                    break;
                }
            }
            if (targetKeyBucket == null) {
                throw new IllegalStateException("New split index not match, new split index=" + newSplitIndex);
            }

            boolean isPut = targetKeyBucket.put(keyBytes, cellHashValue, valueBytes, null);
            if (!isPut) {
                throw new BucketFullException("Split put fail, key=" + new String(keyBytes));
            }

            // clear old cell
            var cellCount = kvMeta.cellCount();
            clearCell(i, cellCount);
            size--;
            cellCost -= cellCount;
        }

        for (var splitKeyBucket : keyBuckets) {
            splitKeyBucket.updateSeq();
        }

        long costT = System.nanoTime() - begin;
        // reduce log
        if (slot == 0 && bucketIndex % 1024 == 0) {
            log.info("Split cost time={}us, capacity={}, size={}, load factor={}, slot={}, bucket index={}, new split number={}",
                    costT / 1000, capacity, size, loadFactor(), slot, bucketIndex, newSplitNumber);
        }
        lastSplitCostNanos = costT;

        // for debug, need delete
//        for (int i = 0; i < keyBuckets.length; i++) {
//            System.out.println(keyBuckets[i]);
//        }
        return keyBuckets;
    }

    private void updateSeq() {
        lastUpdateSeq = snowFlake.nextId();
    }

    private record CanPutResult(boolean flag, boolean isUpdate) {
    }

    public boolean put(byte[] keyBytes, long keyHash, byte[] valueBytes, KeyBucket[] afterPutKeyBuckets) {
        double loadFactor = loadFactor();
        if (loadFactor > HIGH_LOAD_FACTOR) {
            if (afterPutKeyBuckets == null) {
                // fix bug here
                // can not split
                throw new BucketFullException("Key bucket is full, " + this);
            }

            var kbArray = split();
            boolean isSplitFail = false;
            for (int i = 0; i < kbArray.length; i++) {
                var x = kbArray[i];
                afterPutKeyBuckets[i] = x;

                if (!isSplitFail) {
                    isSplitFail = x.loadFactor() > HIGH_LOAD_FACTOR;
                }
            }
            if (isSplitFail) {
                for (var x : kbArray) {
                    log.warn("After split, key bucket is still full, one key bucket={}", x);
                }
                throw new BucketFullException("After split, key bucket is full, slot=" + slot + ", bucket index=" + bucketIndex);
            }

            // split number already changed
            int newSplitIndex = (int) Math.abs(keyHash % splitNumber);
            boolean isPut = false;
            boolean putResult = false;
            for (var keyBucket : kbArray) {
                if (newSplitIndex == keyBucket.splitIndex) {
                    isPut = true;
                    putResult = keyBucket.put(keyBytes, keyHash, valueBytes, null);
                    if (putResult) {
                        keyBucket.updateSeq();
                    }

                    break;
                }
            }
            if (!isPut) {
                throw new IllegalStateException("New split index not match, new split index=" + newSplitIndex);
            }
            return putResult;
        }

        var kvMeta = new KVMeta(0, (short) keyBytes.length, (byte) valueBytes.length);
        int cellCount = kvMeta.cellCount();
        if (cellCount >= INIT_CAPACITY) {
            throw new BucketFullException("Key with value bytes too large, key length=" + keyBytes.length
                    + ", value length=" + valueBytes.length);
        }

        var maxFindCellTimes = capacity / 2;

        int beginCellIndex = (int) Math.abs(keyHash % capacity);
        boolean isUpdate = false;

        int putCellIndex = -1;
        for (int i = 0; i < maxFindCellTimes; i++) {
            int targetCellIndex = beginCellIndex + i;
            if (targetCellIndex >= capacity) {
                targetCellIndex = targetCellIndex - capacity;
            }

            var canPutResult = canPut(keyBytes, keyHash, targetCellIndex, cellCount);
            if (canPutResult.flag) {
                putCellIndex = targetCellIndex;
                isUpdate = canPutResult.isUpdate;
                break;
            }
        }

        if (putCellIndex == -1) {
            var beginCellIndex2 = beginCellIndex + capacity / 2;
            if (beginCellIndex2 >= capacity) {
                beginCellIndex2 = beginCellIndex2 - capacity;
            }

            for (int i = 0; i < maxFindCellTimes; i++) {
                int targetCellIndex = beginCellIndex2 + i;
                if (targetCellIndex >= capacity) {
                    targetCellIndex = targetCellIndex - capacity;
                }

                var canPutResult = canPut(keyBytes, keyHash, targetCellIndex, cellCount);
                if (canPutResult.flag) {
                    putCellIndex = targetCellIndex;
                    isUpdate = canPutResult.isUpdate;
                    break;
                }
            }
        }

        if (putCellIndex == -1) {
            if (loadFactor < LOW_LOAD_FACTOR) {
                log.warn("Key bucket is full, slot={}, bucket index={}, capacity={}, size={}, load factor={}",
                        slot, bucketIndex, capacity, size, loadFactor);
            }
            throw new BucketFullException("Key bucket is full, slot=" + slot + ", bucket index=" + bucketIndex);
        }

        putTo(putCellIndex, cellCount, keyHash, keyBytes, valueBytes);
        if (!isUpdate) {
            size++;
            cellCost += cellCount;
        }

        updateSeq();
        return true;
    }

    private void putTo(int putCellIndex, int cellCount, long keyHash, byte[] keyBytes, byte[] valueBytes) {
        buffer.putLong(putCellIndex * HASH_VALUE_LENGTH, keyHash);
        for (int i = 1; i < cellCount; i++) {
            buffer.putLong((putCellIndex + i) * HASH_VALUE_LENGTH, PRE_KEY);
        }

        // reset old PRE_KEY to NO_KEY
        buffer.position((putCellIndex + cellCount) * HASH_VALUE_LENGTH);
        int cellIndex = putCellIndex + cellCount;
        while (cellIndex < capacity && buffer.getLong() == PRE_KEY) {
            buffer.putLong(buffer.position() - HASH_VALUE_LENGTH, NO_KEY);
            cellIndex++;
        }

        int cellOffset = firstCellOffset() + putCellIndex * ONE_CELL_LENGTH;
        buffer.position(cellOffset);
        buffer.putShort((short) keyBytes.length);
        buffer.put(keyBytes);
        // number or short value or pvm, 1 byte is enough
        buffer.put((byte) valueBytes.length);
        buffer.put(valueBytes);
    }

    private CanPutResult canPut(byte[] keyBytes, long keyHash, int cellIndex, int cellCount) {
        // cell index already in range [0, capacity - ceilCount]
        var cellHashValue = buffer.getLong(cellIndex * HASH_VALUE_LENGTH);
        if (cellHashValue == NO_KEY) {
            var flag = isCellAvailableN(cellIndex, cellCount, false);
            return new CanPutResult(flag, false);
        } else if (cellHashValue == PRE_KEY) {
            return new CanPutResult(false, false);
        } else {
            if (cellHashValue != keyHash) {
                return new CanPutResult(false, false);
            }

            var cellOffset = firstCellOffset() + cellIndex * ONE_CELL_LENGTH;
            var matchMeta = keyMatch(keyBytes, cellOffset);
            if (matchMeta != null) {
                // update
                var flag = isCellAvailableN(cellIndex + 1, cellCount - 1, true);
                return new CanPutResult(flag, true);
            } else {
                // already occupied
                return new CanPutResult(false, false);
            }
        }
    }

    private boolean isCellAvailableN(int cellIndex, int cellCount, boolean isForUpdate) {
        for (int i = 0; i < cellCount; i++) {
            int targetCellIndex = cellIndex + i;
            if (targetCellIndex >= capacity) {
                return false;
            }

            var cellHashValue = buffer.getLong(targetCellIndex * HASH_VALUE_LENGTH);
            if (isForUpdate) {
                if (cellHashValue != PRE_KEY && cellHashValue != NO_KEY) {
                    return false;
                }
            } else if (cellHashValue != NO_KEY) {
                return false;
            }
        }
        return true;
    }

    public byte[] getValueByKey(byte[] keyBytes, long keyHash) {
        if (size == 0) {
            return null;
        }

        var maxFindCellTimes = capacity / 2;

        int beginCellIndex = (int) Math.abs(keyHash % capacity);
        for (int i = 0; i < maxFindCellTimes; i++) {
            int targetCellIndex = beginCellIndex + i;
            if (targetCellIndex >= capacity) {
                targetCellIndex = targetCellIndex - capacity;
            }

            var valueBytes = getValueByKeyWithCellIndex(keyBytes, keyHash, targetCellIndex);
            if (valueBytes != null) {
                return valueBytes;
            }
        }

        var beginCellIndex2 = beginCellIndex + capacity / 2;
        if (beginCellIndex2 >= capacity) {
            beginCellIndex2 = beginCellIndex2 - capacity;
        }
        for (int i = 0; i < maxFindCellTimes; i++) {
            int targetCellIndex = beginCellIndex2 + i;
            if (targetCellIndex >= capacity) {
                targetCellIndex = targetCellIndex - capacity;
            }

            var valueBytes = getValueByKeyWithCellIndex(keyBytes, keyHash, targetCellIndex);
            if (valueBytes != null) {
                return valueBytes;
            }
        }

        return null;
    }

    public byte[] getValueByKeyWithCellIndex(byte[] keyBytes, long keyHash, int cellIndex) {
        var cellHashValue = buffer.getLong(cellIndex * HASH_VALUE_LENGTH);
        // NO_KEY or PRE_KEY
        if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
            return null;
        }
        if (cellHashValue != keyHash) {
            return null;
        }

        var cellOffset = firstCellOffset() + cellIndex * ONE_CELL_LENGTH;
        var matchMeta = keyMatch(keyBytes, cellOffset);
        if (matchMeta == null) {
            return null;
        }

        byte[] valueBytes = new byte[matchMeta.valueLength];
        buffer.position(matchMeta.valueOffset()).get(valueBytes);
        return valueBytes;
    }

    private void clearCell(int beginCellIndex, int cellCount) {
        for (int i = 0; i < cellCount; i++) {
            var cellIndex = beginCellIndex + i;
            buffer.putLong(cellIndex * HASH_VALUE_LENGTH, NO_KEY);
        }

        // set 0 for better compress ratio
        var beginCellOffset = firstCellOffset() + beginCellIndex * ONE_CELL_LENGTH;
        var bytes0 = new byte[ONE_CELL_LENGTH * cellCount];

        // do not change position
        buffer.put(beginCellOffset, bytes0);
    }

    public boolean del(byte[] keyBytes, long keyHash) {
        var maxFindCellTimes = capacity / 2;

        int beginCellIndex = (int) Math.abs(keyHash % capacity);
        for (int i = 0; i < maxFindCellTimes; i++) {
            int targetCellIndex = beginCellIndex + i;
            if (targetCellIndex >= capacity) {
                targetCellIndex = targetCellIndex - capacity;
            }

            var cellHashValue = buffer.getLong(targetCellIndex * HASH_VALUE_LENGTH);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }
            if (cellHashValue != keyHash) {
                continue;
            }

            var cellOffset = firstCellOffset() + targetCellIndex * ONE_CELL_LENGTH;
            var matchMeta = keyMatch(keyBytes, cellOffset);
            if (matchMeta != null) {
                var cellCount = matchMeta.cellCount();
                clearCell(targetCellIndex, cellCount);
                size--;
                cellCost -= cellCount;
                updateSeq();
                return true;
            } else {
                // key masked value conflict, need fix, todo
                log.warn("Key masked value conflict, key masked value={}, target cell index={}, key={}, slot={}, bucket index={}",
                        keyHash, targetCellIndex, new String(keyBytes), slot, bucketIndex);
            }
        }

        var beginCellIndex2 = beginCellIndex + capacity / 2;
        if (beginCellIndex2 >= capacity) {
            beginCellIndex2 = beginCellIndex2 - capacity;
        }
        for (int i = 0; i < maxFindCellTimes; i++) {
            int targetCellIndex = beginCellIndex2 + i;
            if (targetCellIndex >= capacity) {
                targetCellIndex = targetCellIndex - capacity;
            }

            var cellHashValue = buffer.getLong(targetCellIndex * HASH_VALUE_LENGTH);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }
            if (cellHashValue != keyHash) {
                continue;
            }

            var cellOffset = firstCellOffset() + targetCellIndex * ONE_CELL_LENGTH;
            var matchMeta = keyMatch(keyBytes, cellOffset);
            if (matchMeta != null) {
                var cellCount = matchMeta.cellCount();
                clearCell(targetCellIndex, cellCount);
                size--;
                cellCost -= cellCount;
                updateSeq();
                return true;
            } else {
                // key masked value conflict, need fix, todo
                log.warn("Key masked value conflict, key masked value={}, target cell index={}, key={}, slot={}, bucket index={}",
                        keyHash, targetCellIndex, new String(keyBytes), slot, bucketIndex);
            }
        }

        return false;
    }

    private KVMeta keyMatch(byte[] keyBytes, int offset) {
        // compare key length first
        if (keyBytes.length != buffer.getShort(offset)) {
            return null;
        }

        // compare key bytes
        int afterKeyLengthOffset = offset + Short.BYTES;

        // compare one by one
        for (int i = 0; i < keyBytes.length; i++) {
            if (keyBytes[i] != buffer.get(i + afterKeyLengthOffset)) {
                return null;
            }
        }

        // compare key bytes in one time, Arrays.equals is JIT optimized
//        var toCompareKeyBytes = new byte[keyBytes.length];
//        buffer.position(afterKeyLengthOffset).get(toCompareKeyBytes);
//        if (!Arrays.equals(keyBytes, toCompareKeyBytes)) {
//            return null;
//        }

        return new KVMeta(offset, (short) keyBytes.length, buffer.get(offset + Short.BYTES + keyBytes.length));
    }

}
