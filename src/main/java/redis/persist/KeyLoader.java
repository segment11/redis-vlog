package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import redis.ConfForSlot;
import redis.KeyHash;
import redis.SnowFlake;
import redis.metric.SimpleGauge;
import redis.repl.content.ToMasterExistsSegmentMeta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static redis.persist.LocalPersist.PAGE_SIZE;

public class KeyLoader {
    private static final int PAGE_NUMBER_PER_BUCKET = 1;
    public static final int KEY_BUCKET_ONE_COST_SIZE = PAGE_NUMBER_PER_BUCKET * PAGE_SIZE;

    // one split file max 2GB, 2 * 1024 * 1024 / 4 = 524288
    // one split index one file
    static final int MAX_KEY_BUCKET_COUNT_PER_FD = 2 * 1024 * 1024 / 4;

    // for unit test
    public KeyLoader(byte slot, int bucketsPerSlot, File slotDir, SnowFlake snowFlake) {
        this(slot, bucketsPerSlot, slotDir, snowFlake, null);
    }

    public KeyLoader(byte slot, int bucketsPerSlot, File slotDir, SnowFlake snowFlake, OneSlot oneSlot) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.bucketsPerSlot = bucketsPerSlot;
        this.slotDir = slotDir;
        this.snowFlake = snowFlake;
        this.oneSlot = oneSlot;

        this.initMetricsCollect();
    }

    private final byte slot;
    private final String slotStr;
    final int bucketsPerSlot;
    private final File slotDir;
    final SnowFlake snowFlake;

    private final OneSlot oneSlot;

    MetaKeyBucketSplitNumber metaKeyBucketSplitNumber;

    byte[] getMetaKeyBucketSplitNumberBatch(int beginBucketIndex, int bucketCount) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot: " + slot + ", begin bucket index: " + beginBucketIndex);
        }

        return metaKeyBucketSplitNumber.getBatch(beginBucketIndex, bucketCount);
    }

    boolean updateMetaKeyBucketSplitNumberBatchIfChanged(int beginBucketIndex, byte[] splitNumberArray) {
        if (beginBucketIndex < 0 || beginBucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot: " + slot + ", begin bucket index: " + beginBucketIndex);
        }

        // if not change, need not an extra ssd io
        // even though random access file use os page cache
        var currentBytes = metaKeyBucketSplitNumber.getBatch(beginBucketIndex, splitNumberArray.length);
        if (Arrays.equals(currentBytes, splitNumberArray)) {
            return false;
        }

        metaKeyBucketSplitNumber.setBatch(beginBucketIndex, splitNumberArray);
        return true;
    }

    public byte maxSplitNumberForRepl() {
        return metaKeyBucketSplitNumber.maxSplitNumber();
    }

    // read only, important
    public byte[] getMetaKeyBucketSplitNumberBytesToSlaveExists() {
        return metaKeyBucketSplitNumber.getInMemoryCachedBytes();
    }

    public void overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(byte[] bytes) {
        metaKeyBucketSplitNumber.overwriteInMemoryCachedBytes(bytes);
    }

    void setMetaKeyBucketSplitNumberForTest(int bucketIndex, byte splitNumber) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot: " + slot + ", begin bucket index: " + bucketIndex);
        }

        metaKeyBucketSplitNumber.setForTest(bucketIndex, splitNumber);
    }

    // split 2 times, 1 * 3 * 3 = 9
    // when get bigger, batch persist pvm, will slot stall and read all 9 files, read and write perf will be bad
    // end to end read perf ok, because only read one key bucket and lru cache
    // increase buckets per slot value, then will split fewer times, but will cost more wal memory
    // or decrease wal delay persist value size, then will once put less key values, may be better for latency
    public static final byte MAX_SPLIT_NUMBER = 9;
    static final int SPLIT_MULTI_STEP = 3;
    // you can change here, the bigger, key buckets will split more times, like load factor
    // compare to KeyBucket.INIT_CAPACITY
    static final int KEY_OR_CELL_COST_TOLERANCE_COUNT_WHEN_CHECK_SPLIT = 0;

    private LibC libC;
    // index is split index
    FdReadWrite[] fdReadWriteArray;

    private final Logger log = org.slf4j.LoggerFactory.getLogger(KeyLoader.class);

    public static final int BATCH_ONCE_SEGMENT_COUNT_READ_FOR_REPL = ToMasterExistsSegmentMeta.REPL_ONCE_SEGMENT_COUNT;

    StatKeyCountInBuckets statKeyCountInBuckets;

    public short getKeyCountInBucketIndex(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= bucketsPerSlot) {
            throw new IllegalArgumentException("Bucket index out of range, slot: " + slot + ", bucket index: " + bucketIndex);
        }

        return statKeyCountInBuckets.getKeyCountForBucketIndex(bucketIndex);
    }

    public long getKeyCount() {
        return statKeyCountInBuckets.getKeyCount();
    }

    void updateKeyCountBatchCached(int[] keyCountTmp, int beginBucketIndex) {
        if (beginBucketIndex < 0 || beginBucketIndex + keyCountTmp.length > bucketsPerSlot) {
            throw new IllegalArgumentException("Begin bucket index out of range, slot: " + slot + ", begin bucket index: " + beginBucketIndex);
        }

        for (int i = 0; i < keyCountTmp.length; i++) {
            var bucketIndex = beginBucketIndex + i;
            var keyCount = keyCountTmp[i];
            statKeyCountInBuckets.setKeyCountForBucketIndex(bucketIndex, (short) keyCount);
        }
    }

    public void initFds(LibC libC) throws IOException {
        this.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, slotDir);
        this.statKeyCountInBuckets = new StatKeyCountInBuckets(slot, bucketsPerSlot, slotDir);

        this.libC = libC;

        this.fdReadWriteArray = new FdReadWrite[MAX_SPLIT_NUMBER];

        var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();
        this.initFds(maxSplitNumber);
    }

    private void initFds(byte splitNumber) {
        for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
            if (fdReadWriteArray[splitIndex] != null) {
                continue;
            }

            var file = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat");

            // prometheus metric labels use _ instead of -
            var name = "key_bucket_split_" + splitIndex + "_slot_" + slot;
            FdReadWrite fdReadWrite;
            try {
                fdReadWrite = new FdReadWrite(name, libC, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fdReadWrite.initByteBuffers(false);

            fdReadWriteArray[splitIndex] = fdReadWrite;
        }
        log.info("Persist key bucket files fd opened, split number: {}, slot: {}", splitNumber, slot);
    }

    public void cleanUp() {
        if (fdReadWriteArray != null) {
            for (var fdReadWrite : fdReadWriteArray) {
                if (fdReadWrite != null) {
                    fdReadWrite.cleanUp();
                }
            }
        }

        if (metaKeyBucketSplitNumber != null) {
            metaKeyBucketSplitNumber.cleanUp();
            System.out.println("Cleaned up bucket split number");
        }

        if (statKeyCountInBuckets != null) {
            statKeyCountInBuckets.cleanUp();
            System.out.println("Cleaned up key count in buckets");
        }
    }

    // for repl
    public byte[] readKeyBucketBytesBatchToSlaveExists(byte splitIndex, int beginBucketIndex) {
        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            return null;
        }
        return fdReadWrite.readOneInnerForRepl(beginBucketIndex);
    }

    @Deprecated
    public void writeKeyBucketBytesBatchFromMasterExists(byte[] contentBytes) {
        var splitIndex = contentBytes[0];
//            var splitNumber = contentBytes[1];
        var beginBucketIndex = ByteBuffer.wrap(contentBytes, 2, 4).getInt();
        int position = 1 + 1 + 4;
        // left length may be 0
        var leftLength = contentBytes.length - position;

        if (fdReadWriteArray.length <= splitIndex) {
            var oldFdReadWriteArray = fdReadWriteArray;
            fdReadWriteArray = new FdReadWrite[splitIndex + 1];
            System.arraycopy(oldFdReadWriteArray, 0, fdReadWriteArray, 0, oldFdReadWriteArray.length);
        }

        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            initFds((byte) (splitIndex + 1));
            fdReadWrite = fdReadWriteArray[splitIndex];
        }

        if (ConfForSlot.global.pureMemory) {
            if (leftLength == 0) {
                for (int i = 0; i < BATCH_ONCE_SEGMENT_COUNT_READ_FOR_REPL; i++) {
                    fdReadWrite.clearOneKeyBucketToMemory(beginBucketIndex + i);
                }
            } else {
                var bucketCount = leftLength / KEY_BUCKET_ONE_COST_SIZE;
                if (bucketCount != BATCH_ONCE_SEGMENT_COUNT_READ_FOR_REPL) {
                    throw new IllegalStateException("Write pure memory key bucket from master error,  bucket count batch not match, slot: "
                            + slot + ", split index: " + splitIndex + ", begin bucket index: " + beginBucketIndex + ", bucket count: " + bucketCount);
                }

                fdReadWrite.writeOneInnerBatchToMemory(beginBucketIndex, contentBytes, position);
            }
        } else {
            fdReadWrite.writeOneInnerForRepl(beginBucketIndex, contentBytes, position);
        }
        log.info("Write key bucket from master success, slot: {}, split index: {}, begin bucket index: {}",
                slot, splitIndex, beginBucketIndex);
    }

    boolean isBytesValidAsKeyBucket(byte[] bytes, int position) {
        if (bytes == null) {
            return false;
        }

        // init is 0, not write yet
        var firstLong = ByteBuffer.wrap(bytes, position, 8).getLong();
        return firstLong != 0;
    }

    static int getPositionInSharedBytes(int bucketIndex) {
        int firstBucketIndexInTargetWalGroup;
        var mod = bucketIndex % ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (mod != 0) {
            firstBucketIndexInTargetWalGroup = bucketIndex - mod;
        } else {
            firstBucketIndexInTargetWalGroup = bucketIndex;
        }

        return (bucketIndex - firstBucketIndexInTargetWalGroup) * KEY_BUCKET_ONE_COST_SIZE;
    }

    KeyBucket readKeyBucketForSingleKey(int bucketIndex, byte splitIndex, byte splitNumber, long keyHash, boolean isRefreshLRUCache) {
        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            return null;
        }

        var bytes = fdReadWrite.readOneInner(bucketIndex, isRefreshLRUCache);
        if (ConfForSlot.global.pureMemory) {
            // shared bytes
            var position = getPositionInSharedBytes(bucketIndex);
            if (!isBytesValidAsKeyBucket(bytes, position)) {
                return null;
            }
            return new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, bytes, position, snowFlake);
        }

        if (!isBytesValidAsKeyBucket(bytes, 0)) {
            return null;
        }
        return new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, bytes, snowFlake);
    }

    KeyBucket.ValueBytesWithExpireAtAndSeq getValueByKey(int bucketIndex, byte[] keyBytes, long keyHash) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, keyHash, true);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getValueByKey(keyBytes, keyHash);
    }

    // not exact correct when split, just for test or debug, not public
    void putValueByKeyForTest(int bucketIndex, byte[] keyBytes, long keyHash, long expireAt, long seq, byte[] valueBytes) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, keyHash, false);
        if (keyBucket == null) {
            keyBucket = new KeyBucket(slot, bucketIndex, splitIndex, splitNumber, null, snowFlake);
        }

        keyBucket.put(keyBytes, keyHash, expireAt, seq, valueBytes);
        updateKeyBucketInnerForTest(bucketIndex, keyBucket, false);
    }

    // not exact correct when split, just for test or debug, not public
    ArrayList<KeyBucket> readKeyBuckets(int bucketIndex) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        ArrayList<KeyBucket> keyBuckets = new ArrayList<>(splitNumber);

        for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                keyBuckets.add(null);
                continue;
            }

            var bytes = fdReadWrite.readOneInner(bucketIndex, false);
            if (ConfForSlot.global.pureMemory) {
                // shared bytes
                var position = getPositionInSharedBytes(bucketIndex);
                if (!isBytesValidAsKeyBucket(bytes, position)) {
                    keyBuckets.add(null);
                } else {
                    var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, position, snowFlake);
                    keyBuckets.add(keyBucket);
                }
            } else {
                if (!isBytesValidAsKeyBucket(bytes, 0)) {
                    keyBuckets.add(null);
                } else {
                    var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
                    keyBuckets.add(keyBucket);
                }
            }
        }
        return keyBuckets;
    }

    public String readKeyBucketsToStringForDebug(int bucketIndex) {
        var keyBuckets = readKeyBuckets(bucketIndex);

        var sb = new StringBuilder();
        for (var one : keyBuckets) {
            sb.append(one).append("\n");
        }
        return sb.toString();
    }

    private void updateKeyBucketInnerForTest(int bucketIndex, KeyBucket keyBucket, boolean isRefreshLRUCache) {
        var bytes = keyBucket.encode(true);
        var splitIndex = keyBucket.splitIndex;
//        if (bytes.length > KEY_BUCKET_ONE_COST_SIZE) {
//            throw new IllegalStateException("Key bucket bytes size too large, slot: " + slot +
//                    ", bucket index: " + bucketIndex + ", split index: " + splitIndex + ", size: " + bytes.length);
//        }

        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            initFds(keyBucket.splitNumber);
            fdReadWrite = fdReadWriteArray[splitIndex];
        }

        fdReadWrite.writeOneInner(bucketIndex, bytes, isRefreshLRUCache);
    }

    byte[] readBatchInOneWalGroup(byte splitIndex, int beginBucketIndex) {
        var fdReadWrite = fdReadWriteArray[splitIndex];
        if (fdReadWrite == null) {
            return null;
        }
        return fdReadWrite.readOneInnerForKeyBucketsInOneWalGroup(beginBucketIndex);
    }

    public void updatePvmListBatchAfterWriteSegments(int walGroupIndex, ArrayList<PersistValueMeta> pvmList) {
        var inner = new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        inner.putAllPvmList(pvmList);
        updateKeyCountBatchCached(inner.keyCountForStatsTmp, inner.beginBucketIndex);

        var sharedBytesList = inner.writeAfterPutBatch();
        writeSharedBytesList(sharedBytesList, inner.beginBucketIndex);
        updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp);

        if (oneSlot != null) {
            oneSlot.clearKvLRUByWalGroupIndex(walGroupIndex);
        }
    }

    public void persistShortValueListBatchInOneWalGroup(int walGroupIndex, Collection<Wal.V> shortValueList) {
        var inner = new KeyBucketsInOneWalGroup(slot, walGroupIndex, this);
        inner.putAll(shortValueList);
        updateKeyCountBatchCached(inner.keyCountForStatsTmp, inner.beginBucketIndex);

        var sharedBytesList = inner.writeAfterPutBatch();
        writeSharedBytesList(sharedBytesList, inner.beginBucketIndex);
        updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp);

        if (oneSlot != null) {
            oneSlot.clearKvLRUByWalGroupIndex(walGroupIndex);
        }
    }

    void writeSharedBytesList(byte[][] sharedBytesListBySplitIndex, int beginBucketIndex) {
        for (int splitIndex = 0; splitIndex < sharedBytesListBySplitIndex.length; splitIndex++) {
            var sharedBytes = sharedBytesListBySplitIndex[splitIndex];
            if (sharedBytes == null) {
                continue;
            }

            if (fdReadWriteArray.length <= splitIndex) {
                var oldFdReadWriteArray = fdReadWriteArray;
                fdReadWriteArray = new FdReadWrite[splitIndex + 1];
                System.arraycopy(oldFdReadWriteArray, 0, fdReadWriteArray, 0, oldFdReadWriteArray.length);
            }

            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                initFds((byte) (splitIndex + 1));
                fdReadWrite = fdReadWriteArray[splitIndex];
            }

            fdReadWrite.writeOneInnerForKeyBucketsInOneWalGroup(beginBucketIndex, sharedBytes);
        }
    }

    // use wal delay remove instead of remove immediately
    boolean removeSingleKeyForTest(int bucketIndex, byte[] keyBytes, long keyHash) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        var splitIndex = KeyHash.splitIndex(keyHash, splitNumber, bucketIndex);

        var keyBucket = readKeyBucketForSingleKey(bucketIndex, splitIndex, splitNumber, keyHash, false);
        if (keyBucket == null) {
            return false;
        }

        var isDeleted = keyBucket.del(keyBytes, keyHash, true);
        if (isDeleted) {
            updateKeyBucketInnerForTest(bucketIndex, keyBucket, false);
        }

        return isDeleted;
    }

    public void flush() {
        metaKeyBucketSplitNumber.clear();
        statKeyCountInBuckets.clear();

        for (int splitIndex = 0; splitIndex < MAX_SPLIT_NUMBER; splitIndex++) {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                continue;
            }

            fdReadWrite.truncate();
        }
    }

    private final static SimpleGauge keyLoaderInnerGauge = new SimpleGauge("key_loader_inner", "key loader inner",
            "slot");

    static {
        keyLoaderInnerGauge.register();
    }

    private void initMetricsCollect() {
        keyLoaderInnerGauge.addRawGetter(() -> {
            var labelValues = List.of(slotStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("bucket_count", new SimpleGauge.ValueWithLabelValues((double) bucketsPerSlot, labelValues));
            map.put("persist_key_count", new SimpleGauge.ValueWithLabelValues((double) getKeyCount(), labelValues));
            return map;
        });
    }
}
