package redis.persist;

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressStats;
import redis.ConfForSlot;
import redis.SnowFlake;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.persist.KeyLoader.MAX_SPLIT_NUMBER;
import static redis.persist.KeyLoader.SPLIT_MULTI_STEP;

public class KeyBucket {
    private static final short INIT_CAPACITY = 50;
    // if big, wal will cost too much memory
    public static final int MAX_BUCKETS_PER_SLOT = KeyLoader.MAX_KEY_BUCKET_COUNT_PER_FD;
    public static final int DEFAULT_BUCKETS_PER_SLOT = 16384;

    // key length short 2 + key length <= 32 + value length byte 1 + (pvm length 24 or short value case number 17 / string 19 ) < 64
    // if key length > 32, refer CompressedValue.KEY_MAX_LENGTH, one key may cost 2 cells
    // (8 + 8) * 50 + 64 * 50 = 4000, in one 4KB page
    private static final int ONE_CELL_LENGTH = 64;
    private static final int HASH_VALUE_LENGTH = 8;
    private static final int EXPIRE_AT_VALUE_LENGTH = 8;
    private static final int ONE_CELL_META_LENGTH = HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH;
    private static final int SEQ_VALUE_LENGTH = 8;
    // seq long + size short + cell count short + uncompressed length short + compressed length short
    static final int HEADER_LENGTH = SEQ_VALUE_LENGTH + 2 + 2 + 2 + 2;

    private static final int INIT_BYTES_LENGTH = HEADER_LENGTH + INIT_CAPACITY * (ONE_CELL_META_LENGTH + ONE_CELL_LENGTH);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int capacity;
    short size;
    private short cellCost;

    public boolean isFull() {
        return cellCost >= capacity;
    }

    long lastUpdateSeq;

    private final SnowFlake snowFlake;

    long lastSplitCostNanos;

    private int oneCellOffset(int cellIndex) {
        return HEADER_LENGTH + capacity * ONE_CELL_META_LENGTH + cellIndex * ONE_CELL_LENGTH;
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

        this.decompressBytes = new byte[INIT_BYTES_LENGTH];
        this.buffer = ByteBuffer.wrap(decompressBytes);
    }

    interface IterateCallBack {
        void call(long cellHashValue, long expireAt, byte[] keyBytes, byte[] valueBytes);
    }

    void iterate(IterateCallBack callBack) {
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
            buffer.position(oneCellOffset(cellIndex));

            var keyLength = buffer.getShort();
            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.get();
            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            callBack.call(cellHashValue, expireAt, keyBytes, valueBytes);
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

        static int calcCellCount(short keyLength, byte valueLength) {
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
            cellCost = 0;
        } else {
            var bufferInner = ByteBuffer.wrap(compressedData);
            // first 8 bytes is seq
            lastUpdateSeq = bufferInner.getLong();

            // then 2 bytes is size
            size = bufferInner.getShort();
            cellCost = bufferInner.getShort();
            compressStats.updateTmpBucketSize(slot, bucketIndex, splitIndex, size);

            var isCompress = ConfForSlot.global.confBucket.isCompress;
            if (!isCompress) {
                decompressBytes = compressedData;
            } else {
                // then 2 bytes is uncompressed length
                var uncompressedLength = bufferInner.getShort();
                // fix this, Destination buffer is too small, todo
                decompressBytes = new byte[uncompressedLength];

                var compressedSize = bufferInner.getShort();

                long begin = System.nanoTime();
                Zstd.decompressByteArray(decompressBytes, HEADER_LENGTH, uncompressedLength - HEADER_LENGTH,
                        compressedData, HEADER_LENGTH, compressedSize);
                long costT = System.nanoTime() - begin;

                // stats
                // thread not safe, use long adder
                compressStats.decompressCount2.increment();
                compressStats.decompressCostTotalTimeNanos2.add(costT);
            }
        }
        buffer = ByteBuffer.wrap(decompressBytes);
    }

    public byte[] compress() {
        var isCompress = ConfForSlot.global.confBucket.isCompress;
        if (!isCompress) {
            var bufferInner = ByteBuffer.wrap(decompressBytes);
            bufferInner.putLong(lastUpdateSeq).putShort(size).putShort(cellCost);
            return decompressBytes;
        }

        var maxDstSize = (int) Zstd.compressBound(decompressBytes.length - HEADER_LENGTH);
        var dst = new byte[maxDstSize + HEADER_LENGTH];
        int compressedSize = (int) Zstd.compressByteArray(dst, HEADER_LENGTH, maxDstSize,
                decompressBytes, HEADER_LENGTH, decompressBytes.length - HEADER_LENGTH, Zstd.defaultCompressionLevel());

        int afterCompressPersistSize = compressedSize + HEADER_LENGTH;
        if (afterCompressPersistSize > KeyLoader.KEY_BUCKET_ONE_COST_SIZE) {
            throw new IllegalStateException("Compressed size too large, compressed size=" + afterCompressPersistSize);
        }

        // put to cache use minimize size
        // dst include too many 0
        var r = Arrays.copyOfRange(dst, 0, afterCompressPersistSize);
        var bufferInner = ByteBuffer.wrap(r);
        bufferInner.putLong(lastUpdateSeq).putShort(size).putShort(cellCost)
                .putShort((short) decompressBytes.length).putShort((short) compressedSize);

        // stats
        compressStats.compressedValueSizeTotalCount2.add(size);
        compressStats.compressedValueBodyTotalLength2.add(compressedSize);

        compressStats.updateTmpBucketSize(slot, bucketIndex, splitIndex, size);
        return r;
    }

    private void clearOneExpired(int i) {
        int cellCount = 1;
        for (int cellIndex = i + 1; cellIndex < capacity; cellIndex++) {
            int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
            var nextCellHashValue = buffer.getLong(metaIndex);
            if (nextCellHashValue == PRE_KEY) {
                cellCount++;
            } else {
                break;
            }
        }
        clearCell(i, cellCount);
        size--;
        cellCost -= cellCount;
    }

    String allPrint() {
        var sb = new StringBuilder();
        iterate((keyHash, expireAt, keyBytes, valueBytes) -> sb.append("key=").append(new String(keyBytes))
                .append(", value=").append(PersistValueMeta.isPvm(valueBytes) ? PersistValueMeta.decode(valueBytes) : new String(valueBytes))
                .append(", expireAt=").append(expireAt).append("\n"));
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

        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
            if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                clearOneExpired(cellIndex);
                continue;
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

            var cellOffset = oneCellOffset(cellIndex);
            buffer.position(cellOffset);
            var keyLength = buffer.getShort();
            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.get();
            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            var kvMeta = new KVMeta(cellOffset, keyLength, valueLength);

            boolean isPut = targetKeyBucket.put(keyBytes, cellHashValue, expireAt, valueBytes, null);
            if (!isPut) {
                throw new BucketFullException("Split put fail, key=" + new String(keyBytes));
            }

            // clear old cell
            var cellCount = kvMeta.cellCount();
            clearCell(cellIndex, cellCount);
            size--;
            cellCost -= cellCount;
        }

        for (var splitKeyBucket : keyBuckets) {
            splitKeyBucket.updateSeq();
        }

        long costT = System.nanoTime() - begin;
        // reduce log
        if (slot == 0 && bucketIndex % 1024 == 0) {
            log.info("Split cost time={}us, capacity={}, size={}, cell cost={}, slot={}, bucket index={}, new split number={}",
                    costT / 1000, capacity, size, cellCost, slot, bucketIndex, newSplitNumber);
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

    private record CanPutResult(boolean flag, boolean isUpdate, int needRemoveSameKeyCellIndexAfterPut) {
    }

    public boolean put(byte[] keyBytes, long keyHash, long expireAt, byte[] valueBytes, KeyBucket[] afterPutKeyBuckets) {
        if (cellCost == capacity) {
            if (afterPutKeyBuckets == null) {
                throw new BucketFullException("Key bucket is full, " + this);
            }

            var kbArray = split();
            boolean isSplitFail = false;
            for (int i = 0; i < kbArray.length; i++) {
                var x = kbArray[i];
                afterPutKeyBuckets[i] = x;

                if (!isSplitFail) {
                    isSplitFail = x.cellCost >= x.capacity;
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
                    putResult = keyBucket.put(keyBytes, keyHash, expireAt, valueBytes, null);
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

        int cellCount = KVMeta.calcCellCount((short) keyBytes.length, (byte) valueBytes.length);
        if (cellCount >= INIT_CAPACITY) {
            throw new BucketFullException("Key with value bytes too large, key length=" + keyBytes.length
                    + ", value length=" + valueBytes.length);
        }

        int needRemoveSameKeyCellIndexAfterPut = -1;

        boolean isUpdate = false;
        int putToCellIndex = -1;
        for (int i = 0; i < capacity; i++) {
            var canPutResult = canPut(keyBytes, keyHash, i, cellCount);
            if (canPutResult.flag) {
                putToCellIndex = i;
                isUpdate = canPutResult.isUpdate;
                break;
            }

            if (canPutResult.needRemoveSameKeyCellIndexAfterPut != -1) {
                needRemoveSameKeyCellIndexAfterPut = canPutResult.needRemoveSameKeyCellIndexAfterPut;
            }
        }

        if (putToCellIndex == -1) {
            throw new BucketFullException("Key bucket is full, slot=" + slot + ", bucket index=" + bucketIndex);
        }

        putTo(putToCellIndex, cellCount, keyHash, expireAt, keyBytes, valueBytes);
        if (!isUpdate) {
            size++;
            cellCost += cellCount;
        }

        if (needRemoveSameKeyCellIndexAfterPut != -1) {
            buffer.position(oneCellOffset(needRemoveSameKeyCellIndexAfterPut));

            var keyLength = buffer.getShort();
            buffer.position(buffer.position() + keyLength);
            var valueLength = buffer.get();

            int cellCountSameKey = KVMeta.calcCellCount(keyLength, valueLength);
            clearCell(needRemoveSameKeyCellIndexAfterPut, cellCountSameKey);
        }

        // clear same key cell after put
        int nextCellIndex = putToCellIndex + cellCount;
        for (int i = nextCellIndex; i < capacity; i++) {
            var kvMetaSameKey = isCellUseTargetKey(keyBytes, keyHash, i);
            if (kvMetaSameKey != null) {
                clearCell(i, kvMetaSameKey.cellCount());
                size--;
                cellCost -= kvMetaSameKey.cellCount();
            }
        }

        updateSeq();
        return true;
    }

    private void putTo(int putToCellIndex, int cellCount, long keyHash, long expireAt, byte[] keyBytes, byte[] valueBytes) {
        int metaIndex = HEADER_LENGTH + putToCellIndex * ONE_CELL_META_LENGTH;
        buffer.putLong(metaIndex, keyHash);
        buffer.putLong(metaIndex + HASH_VALUE_LENGTH, expireAt);

        for (int i = 1; i < cellCount; i++) {
            int nextIndex = HEADER_LENGTH + (putToCellIndex + i) * ONE_CELL_META_LENGTH;
            buffer.putLong(nextIndex, PRE_KEY);
            buffer.putLong(nextIndex + HASH_VALUE_LENGTH, NO_EXPIRE);
        }

        // reset old PRE_KEY to NO_KEY
        int beginResetOldCellIndex = putToCellIndex + cellCount;
        buffer.position(HEADER_LENGTH + beginResetOldCellIndex * ONE_CELL_META_LENGTH);
        while (beginResetOldCellIndex < capacity) {
            var targetCellHashValue = buffer.getLong();
            buffer.position(buffer.position() + EXPIRE_AT_VALUE_LENGTH);

            if (targetCellHashValue != PRE_KEY) {
                break;
            }

            buffer.putLong(buffer.position() - EXPIRE_AT_VALUE_LENGTH, NO_EXPIRE);
            buffer.putLong(buffer.position() - EXPIRE_AT_VALUE_LENGTH - HASH_VALUE_LENGTH, NO_KEY);
            beginResetOldCellIndex++;
        }

        var cellOffset = oneCellOffset(putToCellIndex);
        buffer.position(cellOffset);
        buffer.putShort((short) keyBytes.length);
        buffer.put(keyBytes);
        // number or short value or pvm, 1 byte is enough
        buffer.put((byte) valueBytes.length);
        buffer.put(valueBytes);
    }

    private KVMeta isCellUseTargetKey(byte[] keyBytes, long keyHash, int cellIndex) {
        int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
        var cellHashValue = buffer.getLong(metaIndex);

        if (cellHashValue != keyHash) {
            return null;
        }

        return keyMatch(keyBytes, oneCellOffset(cellIndex));
    }

    private CanPutResult canPut(byte[] keyBytes, long keyHash, int cellIndex, int cellCount) {
        int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
        var cellHashValue = buffer.getLong(metaIndex);
        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);

        if (cellHashValue == NO_KEY) {
            var flag = isCellAvailableN(cellIndex, cellCount, false);
            return new CanPutResult(flag, false, -1);
        } else if (cellHashValue == PRE_KEY) {
            return new CanPutResult(false, false, -1);
        } else {
            if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                clearOneExpired(cellIndex);
                // check again
                return canPut(keyBytes, keyHash, cellIndex, cellCount);
            }

            if (cellHashValue != keyHash) {
                return new CanPutResult(false, false, -1);
            }

            var matchMeta = keyMatch(keyBytes, oneCellOffset(cellIndex));
            if (matchMeta != null) {
                // update
                var flag = isCellAvailableN(cellIndex + 1, cellCount - 1, true);
                // need clear old cell after put if key match but can not put this time
                return new CanPutResult(flag, true, flag ? -1 : cellIndex);
            } else {
                // hash conflict
                return new CanPutResult(false, false, -1);
            }
        }
    }

    private boolean isCellAvailableN(int cellIndex, int cellCount, boolean isForUpdate) {
        for (int i = 0; i < cellCount; i++) {
            int nextCellIndex = cellIndex + i;
            if (nextCellIndex >= capacity) {
                return false;
            }

            int metaIndex = HEADER_LENGTH + nextCellIndex * ONE_CELL_META_LENGTH;
            var cellHashValue = buffer.getLong(metaIndex);
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

    record ValueBytesWithExpireAt(byte[] valueBytes, long expireAt) {
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }
    }

    public ValueBytesWithExpireAt getValueByKey(byte[] keyBytes, long keyHash) {
        if (size == 0) {
            return null;
        }

        for (int i = 0; i < capacity; i++) {
            var r = getValueByKeyWithCellIndex(keyBytes, keyHash, i);
            if (r != null) {
                return r;
            }
        }

        return null;
    }

    private ValueBytesWithExpireAt getValueByKeyWithCellIndex(byte[] keyBytes, long keyHash, int cellIndex) {
        int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
        var cellHashValue = buffer.getLong(metaIndex);
        // NO_KEY or PRE_KEY
        if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
            return null;
        }
        if (cellHashValue != keyHash) {
            return null;
        }

        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);

        var matchMeta = keyMatch(keyBytes, oneCellOffset(cellIndex));
        if (matchMeta == null) {
            return null;
        }

        byte[] valueBytes = new byte[matchMeta.valueLength];
        buffer.position(matchMeta.valueOffset()).get(valueBytes);
        return new ValueBytesWithExpireAt(valueBytes, expireAt);
    }

    private void clearCell(int beginCellIndex, int cellCount) {
        for (int i = 0; i < cellCount; i++) {
            var nextCellIndex = beginCellIndex + i;
            int metaIndex = HEADER_LENGTH + nextCellIndex * ONE_CELL_META_LENGTH;
            buffer.putLong(metaIndex, NO_KEY);
            buffer.putLong(metaIndex + HASH_VALUE_LENGTH, NO_EXPIRE);
        }

        // set 0 for better compress ratio
        var beginCellOffset = oneCellOffset(beginCellIndex);
        var bytes0 = new byte[ONE_CELL_LENGTH * cellCount];

        buffer.put(beginCellOffset, bytes0);
    }

    public boolean del(byte[] keyBytes, long keyHash) {
        boolean isDeleted = false;
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }
            if (cellHashValue != keyHash) {
                continue;
            }

            var cellOffset = oneCellOffset(cellIndex);
            var matchMeta = keyMatch(keyBytes, cellOffset);
            if (matchMeta != null) {
                var cellCount = matchMeta.cellCount();
                clearCell(cellIndex, cellCount);
                size--;
                cellCost -= cellCount;
                updateSeq();

                isDeleted = true;
            } else {
                // hash conflict, just continue
                log.warn("Key hash conflict, key hash={}, target cell index={}, key={}, slot={}, bucket index={}",
                        keyHash, cellIndex, new String(keyBytes), slot, bucketIndex);
            }
        }

        return isDeleted;
    }

    private KVMeta keyMatch(byte[] keyBytes, int offset) {
        // compare key length first
        if (keyBytes.length != buffer.getShort(offset)) {
            return null;
        }

        // compare key bytes
        int afterKeyLengthOffset = offset + Short.BYTES;

        var buffer0 = ByteBuffer.wrap(keyBytes);
        var buffer1 = buffer.slice(afterKeyLengthOffset, keyBytes.length);
        if (!buffer0.equals(buffer1)) {
            return null;
        }

        return new KVMeta(offset, (short) keyBytes.length, buffer.get(offset + Short.BYTES + keyBytes.length));
    }

}
