package redis.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.SnowFlake;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.persist.KeyLoader.*;

public class KeyBucket {
    public static final short INIT_CAPACITY = 46;
    // if big, wal will cost too much memory
    public static final int MAX_BUCKETS_PER_SLOT = KeyLoader.MAX_KEY_BUCKET_COUNT_PER_FD;
    public static final int DEFAULT_BUCKETS_PER_SLOT = 16384;

    // key length short 2 + key length <= 32 + value length byte 1 + (pvm length 24 or short value case number 17 / string 19 ) < 64
    // if key length > 32, refer CompressedValue.KEY_MAX_LENGTH, one key may cost 2 cells
    // (8 + 8) * 50 + 64 * 50 = 4000, in one 4KB page
    private static final int ONE_CELL_LENGTH = 64;
    private static final int HASH_VALUE_LENGTH = 8;
    private static final int EXPIRE_AT_VALUE_LENGTH = 8;
    private static final int SEQ_VALUE_LENGTH = 8;
    private static final int ONE_CELL_META_LENGTH = HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH + SEQ_VALUE_LENGTH;
    // seq long + size short + cell count short
    private static final int HEADER_LENGTH = 8 + 2 + 2;

    // just make sure when refactoring
    private static final int INIT_BYTES_LENGTH = HEADER_LENGTH + INIT_CAPACITY * (ONE_CELL_META_LENGTH + ONE_CELL_LENGTH);

    static {
        if (INIT_BYTES_LENGTH > KEY_BUCKET_ONE_COST_SIZE) {
            throw new IllegalStateException("INIT_BYTES_LENGTH > KEY_BUCKET_ONE_COST_SIZE");
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int capacity;
    short size;
    short cellCost;

    public boolean isFull() {
        return cellCost >= capacity;
    }

    long lastUpdateSeq;

    private final SnowFlake snowFlake;

    long lastSplitCostNanos;

    private int oneCellOffset(int cellIndex) {
        return HEADER_LENGTH + capacity * ONE_CELL_META_LENGTH + cellIndex * ONE_CELL_LENGTH;
    }

    private int metaIndex(int cellIndex) {
        return HEADER_LENGTH + cellIndex * ONE_CELL_META_LENGTH;
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
    final byte[] bytes;
    final int position;

    boolean isSharedBytes() {
        return bytes.length != KEY_BUCKET_ONE_COST_SIZE;
    }

    public KeyBucket(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, @Nullable byte[] bytes, SnowFlake snowFlake) {
        this(slot, bucketIndex, splitIndex, splitNumber, bytes, 0, snowFlake);
    }

    public KeyBucket(byte slot, int bucketIndex, byte splitIndex, byte splitNumber, @Nullable byte[] sharedBytes, int position, SnowFlake snowFlake) {
        this.slot = slot;
        this.bucketIndex = bucketIndex;
        this.splitIndex = splitIndex;
        this.splitNumber = splitNumber;

        this.capacity = INIT_CAPACITY;
        this.size = 0;
        this.cellCost = 0;
        this.snowFlake = snowFlake;

        if (sharedBytes == null) {
            this.bytes = new byte[KEY_BUCKET_ONE_COST_SIZE];
            this.position = 0;
            this.buffer = ByteBuffer.wrap(this.bytes, this.position, KEY_BUCKET_ONE_COST_SIZE);
        } else {
            if (sharedBytes.length % KEY_BUCKET_ONE_COST_SIZE != 0) {
                throw new IllegalStateException("Key bucket shared bytes length must be multiple of " + KEY_BUCKET_ONE_COST_SIZE);
            }

            if (sharedBytes.length <= position) {
                this.bytes = new byte[KEY_BUCKET_ONE_COST_SIZE];
                this.position = 0;
                this.buffer = ByteBuffer.wrap(this.bytes, this.position, KEY_BUCKET_ONE_COST_SIZE);
            } else {
                this.bytes = sharedBytes;
                this.position = position;
                this.buffer = ByteBuffer.wrap(this.bytes, this.position, KEY_BUCKET_ONE_COST_SIZE).slice();
            }
        }

        this.lastUpdateSeq = buffer.getLong();
        this.size = buffer.getShort();
        this.cellCost = buffer.getShort();
    }

    interface IterateCallBack {
        void call(long cellHashValue, long expireAt, byte[] keyBytes, byte[] valueBytes);
    }

    void iterate(IterateCallBack callBack) {
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
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

    public byte[] encode() {
        buffer.position(0).putLong(lastUpdateSeq).putShort(size).putShort(cellCost);
        // shared bytes
        if (bytes.length != KEY_BUCKET_ONE_COST_SIZE) {
            var dst = new byte[KEY_BUCKET_ONE_COST_SIZE];
            System.arraycopy(bytes, position, dst, 0, KEY_BUCKET_ONE_COST_SIZE);
            return dst;
        } else {
            return bytes;
        }
    }

    private void clearOneExpired(int i) {
        int cellCount = 1;
        for (int cellIndex = i + 1; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
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
                    newSplitNumber, null, 0, this.snowFlake);
            keyBuckets[i] = splitKeyBucket;
        }

        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
            var cellHashValue = buffer.getLong(metaIndex);
            if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
                continue;
            }

            var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
            if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                clearOneExpired(cellIndex);
                continue;
            }

            var seq = buffer.getLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH);

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

            boolean isPut = targetKeyBucket.put(keyBytes, cellHashValue, expireAt, seq, valueBytes, null).isPut;
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

    private record CanPutResult(boolean flag, boolean isUpdate) {
    }

    // is put is always true
    record DoPutResult(boolean isPut, boolean isUpdate) {
    }

    public DoPutResult put(byte[] keyBytes, long keyHash, long expireAt, long seq, byte[] valueBytes, KeyBucket[] afterPutKeyBuckets) {
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
            DoPutResult doPutResult = null;
            for (var keyBucketAfterSplit : kbArray) {
                if (newSplitIndex == keyBucketAfterSplit.splitIndex) {
                    doPutResult = keyBucketAfterSplit.put(keyBytes, keyHash, expireAt, seq, valueBytes, null);
                    keyBucketAfterSplit.updateSeq();
                    break;
                }
            }
            if (doPutResult == null) {
                throw new IllegalStateException("New split index not match, new split index=" + newSplitIndex);
            }
            return doPutResult;
        }

        int cellCount = KVMeta.calcCellCount((short) keyBytes.length, (byte) valueBytes.length);
        if (cellCount >= INIT_CAPACITY) {
            throw new BucketFullException("Key with value bytes too large, key length=" + keyBytes.length
                    + ", value length=" + valueBytes.length);
        }

        // all in memory, performance is not a problem
        var isExists = del(keyBytes, keyHash, false);

        boolean isUpdate = false;
        int putToCellIndex = -1;
        for (int i = 0; i < capacity; i++) {
            var canPutResult = canPut(keyBytes, keyHash, i, cellCount);
            if (canPutResult.flag) {
                putToCellIndex = i;
                isUpdate = isExists;
                break;
            }
        }

        if (putToCellIndex == -1) {
            if (isExists) {
                // cell count is not enough ? key is not change, will not happen
                // already deleted, data missing, SHIT
                log.error("!!!Key bucket put fail but already delete old one already saved, need put manually, key: {}", new String(keyBytes));
                log.error("!!!Key bucket put fail but already delete old one already saved, need put manually, key: {}", new String(keyBytes));
                log.error("!!!Key bucket put fail but already delete old one already saved, need put manually, key: {}", new String(keyBytes));
            }
            throw new BucketFullException("Key bucket is full, this: " + this);
        }

        putTo(putToCellIndex, cellCount, keyHash, expireAt, seq, keyBytes, valueBytes);
        size++;
        cellCost += cellCount;

        updateSeq();
        return new DoPutResult(true, isUpdate);
    }

    private void putTo(int putToCellIndex, int cellCount, long keyHash, long expireAt, long seq, byte[] keyBytes, byte[] valueBytes) {
        int metaIndex = metaIndex(putToCellIndex);
        buffer.putLong(metaIndex, keyHash);
        buffer.putLong(metaIndex + HASH_VALUE_LENGTH, expireAt);
        buffer.putLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH, seq);

        for (int i = 1; i < cellCount; i++) {
            int nextIndex = metaIndex(putToCellIndex + i);
            buffer.putLong(nextIndex, PRE_KEY);
            buffer.putLong(nextIndex + HASH_VALUE_LENGTH, NO_EXPIRE);
            buffer.putLong(nextIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH, 0L);
        }

        // reset old PRE_KEY to NO_KEY
        int beginResetOldCellIndex = putToCellIndex + cellCount;
        buffer.position(metaIndex(beginResetOldCellIndex));
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
        int metaIndex = metaIndex(cellIndex);
        var cellHashValue = buffer.getLong(metaIndex);

        if (cellHashValue != keyHash) {
            return null;
        }

        return keyMatch(keyBytes, oneCellOffset(cellIndex));
    }

    private CanPutResult canPut(byte[] keyBytes, long keyHash, int cellIndex, int cellCount) {
        int metaIndex = metaIndex(cellIndex);
        var cellHashValue = buffer.getLong(metaIndex);
        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);

        if (cellHashValue == NO_KEY) {
            var flag = isCellAvailableN(cellIndex, cellCount, false);
            return new CanPutResult(flag, false);
        } else if (cellHashValue == PRE_KEY) {
            return new CanPutResult(false, false);
        } else {
            if (expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis()) {
                clearOneExpired(cellIndex);
                // check again
                return canPut(keyBytes, keyHash, cellIndex, cellCount);
            }

            if (cellHashValue != keyHash) {
                return new CanPutResult(false, false);
            }

            var matchMeta = keyMatch(keyBytes, oneCellOffset(cellIndex));
            if (matchMeta != null) {
                // update
                var flag = isCellAvailableN(cellIndex + 1, cellCount - 1, true);
                return new CanPutResult(flag, true);
            } else {
                // hash conflict
                return new CanPutResult(false, false);
            }
        }
    }

    private boolean isCellAvailableN(int cellIndex, int cellCount, boolean isForUpdate) {
        for (int i = 0; i < cellCount; i++) {
            int nextCellIndex = cellIndex + i;
            if (nextCellIndex >= capacity) {
                return false;
            }

            int metaIndex = metaIndex(nextCellIndex);
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

    record ValueBytesWithExpireAtAndSeq(byte[] valueBytes, long expireAt, long seq) {
        boolean isExpired() {
            return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
        }
    }

    public ValueBytesWithExpireAtAndSeq getValueByKey(byte[] keyBytes, long keyHash) {
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

    private ValueBytesWithExpireAtAndSeq getValueByKeyWithCellIndex(byte[] keyBytes, long keyHash, int cellIndex) {
        int metaIndex = metaIndex(cellIndex);
        var cellHashValue = buffer.getLong(metaIndex);
        // NO_KEY or PRE_KEY
        if (cellHashValue == NO_KEY || cellHashValue == PRE_KEY) {
            return null;
        }
        if (cellHashValue != keyHash) {
            return null;
        }

        var expireAt = buffer.getLong(metaIndex + HASH_VALUE_LENGTH);
        var seq = buffer.getLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH);

        var matchMeta = keyMatch(keyBytes, oneCellOffset(cellIndex));
        if (matchMeta == null) {
            return null;
        }

        byte[] valueBytes = new byte[matchMeta.valueLength];
        buffer.position(matchMeta.valueOffset()).get(valueBytes);
        return new ValueBytesWithExpireAtAndSeq(valueBytes, expireAt, seq);
    }

    private void clearCell(int beginCellIndex, int cellCount) {
        for (int i = 0; i < cellCount; i++) {
            var nextCellIndex = beginCellIndex + i;
            int metaIndex = metaIndex(nextCellIndex);
            buffer.putLong(metaIndex, NO_KEY);
            buffer.putLong(metaIndex + HASH_VALUE_LENGTH, NO_EXPIRE);
            buffer.putLong(metaIndex + HASH_VALUE_LENGTH + EXPIRE_AT_VALUE_LENGTH, 0L);
        }

        // set 0 for better compress ratio
        var beginCellOffset = oneCellOffset(beginCellIndex);
        var bytes0 = new byte[ONE_CELL_LENGTH * cellCount];

        buffer.put(beginCellOffset, bytes0);
    }

    public boolean del(byte[] keyBytes, long keyHash, boolean doUpdateSeq) {
        boolean isDeleted = false;
        for (int cellIndex = 0; cellIndex < capacity; cellIndex++) {
            int metaIndex = metaIndex(cellIndex);
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
                if (doUpdateSeq) {
                    updateSeq();
                }

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
