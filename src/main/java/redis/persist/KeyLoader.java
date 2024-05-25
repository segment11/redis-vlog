package redis.persist;

import jnr.posix.LibC;
import redis.CompressStats;
import redis.ConfForSlot;
import redis.SnowFlake;
import redis.metric.SimpleGauge;
import redis.repl.MasterUpdateCallback;
import redis.repl.content.ToMasterExistsSegmentMeta;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static redis.persist.LocalPersist.PAGE_SIZE;

public class KeyLoader extends ThreadSafeCaller {
    private static final int PAGE_NUMBER_PER_BUCKET = 1;
    static final int KEY_BUCKET_ONE_COST_SIZE = PAGE_NUMBER_PER_BUCKET * PAGE_SIZE;

    // one split file max 2GB, 2 * 1024 * 1024 / 4 = 524288
    // one split index one file
    static final int MAX_KEY_BUCKET_COUNT_PER_FD = 2 * 1024 * 1024 / 4;

    public KeyLoader(byte slot, int bucketsPerSlot, File slotDir, SnowFlake snowFlake,
                     MasterUpdateCallback masterUpdateCallback, DynConfig dynConfig) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.bucketsPerSlot = bucketsPerSlot;
        this.slotDir = slotDir;
        this.snowFlake = snowFlake;
        this.masterUpdateCallback = masterUpdateCallback;

        this.dynConfig = dynConfig;

        this.initMetricsCollect();
    }

    private final byte slot;
    private final String slotStr;
    private final int bucketsPerSlot;
    private final File slotDir;
    private final SnowFlake snowFlake;
    private final MasterUpdateCallback masterUpdateCallback;

    private final DynConfig dynConfig;

    // use read write better than synchronized
    private MetaKeyBucketSplitNumber metaKeyBucketSplitNumber;

    public byte maxSplitNumber() {
        return metaKeyBucketSplitNumber.maxSplitNumber();
    }

    // read only, important
    public byte[] getMetaKeyBucketSplitNumberBytesToSlaveExists() {
        return metaKeyBucketSplitNumber.getInMemoryCachedBytes();
    }

    public void overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(byte[] bytes) {
        metaKeyBucketSplitNumber.overwriteInMemoryCachedBytes(bytes);
    }

    public void setMetaKeyBucketSplitNumberFromMasterNewly(int bucketIndex, byte splitNumber) {
        metaKeyBucketSplitNumber.set(bucketIndex, splitNumber);
    }

    // split 3 times, 3 * 3 * 3 = 27
    // when 27, batch persist pvm, will slot lock and read all 27 key buckets for target bucket index, write perf bad
    // read perf ok, because only read one key bucket and lru cache
    // increase buckets per slot config value, then will split fewer times, but will cost more memory
    public static final byte MAX_SPLIT_NUMBER = 27;
    static final int SPLIT_MULTI_STEP = 3;

    private final CompressStats compressStats = new CompressStats("key bucket");

    private LibC libC;
    private FdReadWrite[] fdReadWriteArray;

    public static final int BATCH_ONCE_SEGMENT_COUNT_READ_FOR_REPL = ToMasterExistsSegmentMeta.ONCE_SEGMENT_COUNT;

    private long splitCount;
    private long splitCostNanos;

    private StatKeyBucketLastUpdateCount statKeyBucketLastUpdateCount;

    public short getKeyCountInBucketIndex(int bucketIndex) {
        return statKeyBucketLastUpdateCount.getKeyCountInBucketIndex(bucketIndex);
    }

    public long getKeyCount() {
        return statKeyBucketLastUpdateCount.getKeyCount();
    }

    @Override
    String threadName() {
        return "key-loader-" + slot;
    }

    @Override
    ThreadFactory getNextThreadFactory() {
        return ThreadFactoryAssignSupport.getInstance().ForKeyLoader.getNextThreadFactory();
    }

    public void init(LibC libC) throws IOException {
        this.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, bucketsPerSlot, slotDir);
        this.statKeyBucketLastUpdateCount = new StatKeyBucketLastUpdateCount(slot, bucketsPerSlot, slotDir);

        this.libC = libC;

        var fdLength = MAX_SPLIT_NUMBER;
        this.fdReadWriteArray = new FdReadWrite[fdLength];
    }

    public void initAfterEventloopReady() {
        var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();
        this.initFds(maxSplitNumber);
    }

    private void initFds(byte splitNumber) {
        callSync(() -> {
            for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
                if (fdReadWriteArray[splitIndex] != null) {
                    continue;
                }

                var file = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat");

                // prometheus metric labels use _ instead of -
                var name = "key_bucket_split_" + splitIndex + "_slot_" + slot;
                var fdReadWrite = new FdReadWrite(name, libC, file);
                fdReadWrite.initByteBuffers(false);
                fdReadWrite.initEventloop();

                fdReadWriteArray[splitIndex] = fdReadWrite;
            }
            log.info("Persist key bucket files fd opened, split number: {}, slot: {}", splitNumber, slot);
            return 0;
        });
    }

    public void cleanUp() {
        stopEventLoop();

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

        if (statKeyBucketLastUpdateCount != null) {
            statKeyBucketLastUpdateCount.cleanUp();
        }
    }

    public byte[] readKeyBucketBytesBatchToSlaveExists(byte splitIndex, int beginBucketIndex) {
        return callSync(() -> {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                return null;
            }
            return fdReadWrite.readSegmentForRepl(beginBucketIndex);
        });
    }

    public void writeKeyBucketBytesBatchFromMaster(byte[] contentBytes) {
        callSync(() -> {
            var splitIndex = contentBytes[0];
//            var splitNumber = contentBytes[1];
            var beginBucketIndex = ByteBuffer.wrap(contentBytes, 2, 4).getInt();
            int position = 1 + 1 + 4;
            // left length may be 0
            var leftLength = contentBytes.length - position;

            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                initFds((byte) (splitIndex + 1));
                fdReadWrite = fdReadWriteArray[splitIndex];
            }

            if (ConfForSlot.global.pureMemory) {
                if (leftLength == 0) {
                    for (int i = 0; i < BATCH_ONCE_SEGMENT_COUNT_READ_FOR_REPL; i++) {
                        fdReadWrite.clearOneSegmentToMemory(beginBucketIndex + i);
                    }
                } else {
                    var bucketCount = leftLength / KEY_BUCKET_ONE_COST_SIZE;
                    if (bucketCount != BATCH_ONCE_SEGMENT_COUNT_READ_FOR_REPL) {
                        throw new IllegalStateException("Write pure memory key bucket from master error,  bucket count batch not match, slot: "
                                + slot + ", split index: " + splitIndex + ", begin bucket index: " + beginBucketIndex + ", bucket count: " + bucketCount);
                    }

                    fdReadWrite.writeSegmentBatchToMemory(beginBucketIndex, contentBytes, position);
                }
            } else {
                fdReadWrite.writeSegmentForRepl(beginBucketIndex, contentBytes, position);
            }
            log.info("Write key bucket from master success, slot: {}, split index: {}, begin bucket index: {}",
                    slot, splitIndex, beginBucketIndex);
            return 0;
        });
    }

    private boolean isBytesValidAsKeyBucket(byte[] bytes) {
        if (bytes == null) {
            return false;
        }

        // init is 0, not write yet
        var firstLong = ByteBuffer.wrap(bytes, 0, 8).getLong();
        return firstLong != 0;
    }

    private KeyBucket readKeyBucketForSingleKey(int bucketIndex, long keyHash) {
        return callSync(() -> {
            var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
            var splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(keyHash % splitNumber);
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                return null;
            }

            var bytes = fdReadWrite.readSegment(bucketIndex, true);
            if (!isBytesValidAsKeyBucket(bytes)) {
                return null;
            }

            var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
            keyBucket.initWithCompressStats(compressStats);
            return keyBucket;
        });
    }

    public KeyBucket.ValueBytesWithExpireAt getValueByKey(int bucketIndex, byte[] keyBytes, long keyHash) {
        return callSync(() -> {
            var keyBucket = readKeyBucketForSingleKey(bucketIndex, keyHash);
            if (keyBucket == null) {
                return null;
            }

            return keyBucket.getValueByKey(keyBytes, keyHash);
        });
    }

    public void putValueByKeyForTest(int bucketIndex, byte[] keyBytes, long keyHash, long expireAt, byte[] valueBytes) {
        callSync(() -> {
            var keyBucket = readKeyBucketForSingleKey(bucketIndex, keyHash);
            if (keyBucket == null) {
                var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
                var splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(keyHash % splitNumber);

                keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, null, snowFlake);
                keyBucket.initWithCompressStats(compressStats);
            }

            keyBucket.put(keyBytes, keyHash, expireAt, valueBytes, null);
            updateKeyBucketInner(bucketIndex, keyBucket, true);
            return 0;
        });
    }

    public ArrayList<KeyBucket> readKeyBuckets(int bucketIndex) {
        return callSync(() -> {
            var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
            ArrayList<KeyBucket> keyBuckets = new ArrayList<>(splitNumber);

            for (int splitIndex = 0; splitIndex < splitNumber; splitIndex++) {
                var fdReadWrite = fdReadWriteArray[splitIndex];
                if (fdReadWrite == null) {
                    keyBuckets.add(null);
                    continue;
                }

                var bytes = fdReadWrite.readSegment(bucketIndex, false);
                if (!isBytesValidAsKeyBucket(bytes)) {
                    keyBuckets.add(null);
                } else {
                    var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
                    keyBucket.initWithCompressStats(compressStats);
                    keyBuckets.add(keyBucket);
                }
            }
            return keyBuckets;
        });
    }

    public void updateKeyBucketFromMasterNewly(int bucketIndex, byte splitIndex, byte splitNumber, long lastUpdateSeq, byte[] bytes) {
        updateKeyBucketInner(bucketIndex, splitIndex, splitNumber, lastUpdateSeq, bytes, false);
    }

    private void updateKeyBucketInner(int bucketIndex, byte splitIndex, byte splitNumber, long lastUpdateSeq, byte[] bytes, boolean isRefreshLRUCache) {
        if (bytes.length > KEY_BUCKET_ONE_COST_SIZE) {
            throw new IllegalStateException("Key bucket bytes size too large, slot: " + slot +
                    ", bucket index: " + bucketIndex + ", split index: " + splitIndex + ", size: " + bytes.length);
        }

        callSync(() -> {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                initFds(splitNumber);
                fdReadWrite = fdReadWriteArray[splitIndex];
            }

            fdReadWrite.writeSegment(bucketIndex, bytes, isRefreshLRUCache);

            if (masterUpdateCallback != null) {
                masterUpdateCallback.onKeyBucketUpdate(slot, bucketIndex, splitIndex, splitNumber, lastUpdateSeq, bytes);
            }
            return 0;
        });
    }

    private void updateKeyBucketInner(int bucketIndex, KeyBucket keyBucket, boolean isRefreshLRUCache) {
        updateKeyBucketInner(bucketIndex, keyBucket.splitIndex, keyBucket.splitNumber, keyBucket.lastUpdateSeq, keyBucket.compress(), isRefreshLRUCache);
    }

    private interface UpdateBatchCallback {
        void call(final ArrayList<KeyBucket> keyBuckets, final boolean[] putFlags, final byte splitNumber, final boolean isLoadedAll);
    }

    private long updateBatchCount = 0;

    private KeyBucket readKeyBucket(int bucketIndex, int splitIndex, byte splitNumber) {
        return callSync(() -> {
            var fdReadWrite = fdReadWriteArray[splitIndex];
            if (fdReadWrite == null) {
                return null;
            }

            var bytes = fdReadWrite.readSegment(bucketIndex, false);
            if (!isBytesValidAsKeyBucket(bytes)) {
                return null;
            }

            var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
            keyBucket.initWithCompressStats(compressStats);
            return keyBucket;
        });
    }

    private void updateBatch(int bucketIndex, long keyHash, UpdateBatchCallback callback, boolean isRefreshLRUCache) {
        callSync(() -> {
            var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
            ArrayList<KeyBucket> keyBuckets = new ArrayList<>(splitNumber);

            var isSingleKeyUpdate = keyHash != 0;
            // just get one key bucket
            if (isSingleKeyUpdate) {
                var splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(keyHash % splitNumber);
                var keyBucket = readKeyBucket(bucketIndex, splitIndex, splitNumber);
                if (keyBucket != null) {
                    keyBuckets.add(keyBucket);
                }
            } else {
                for (int i = 0; i < splitNumber; i++) {
                    var splitIndex = i;
                    var keyBucket = readKeyBucket(bucketIndex, splitIndex, splitNumber);
                    if (keyBucket != null) {
                        keyBuckets.add(keyBucket);
                    } else {
                        // create one empty key bucket
                        keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, null, snowFlake);
                        keyBucket.initWithCompressStats(compressStats);
                        keyBuckets.add(keyBucket);
                    }
                }
            }

            boolean[] putBackFlags = new boolean[splitNumber];
            callback.call(keyBuckets, putBackFlags, splitNumber, keyHash == 0);

            boolean sizeChanged = false;
            for (int i = 0; i < splitNumber; i++) {
                if (putBackFlags[i]) {
                    var keyBucket = keyBuckets.get(i);
                    updateKeyBucketInner(bucketIndex, keyBucket, isRefreshLRUCache);
                    sizeChanged = true;
                }
            }

            // key count for each key bucket, is not accurate
            if (sizeChanged && !isSingleKeyUpdate) {
                int keyCount = 0;
                for (var keyBucket : keyBuckets) {
                    if (keyBucket != null) {
                        keyCount += keyBucket.size;
                    }
                }

                updateBatchCount++;
                boolean isSync = updateBatchCount % 10 == 0;
                // can be async for better performance, but key count is not accurate
                statKeyBucketLastUpdateCount.setKeyCountInBucketIndex(bucketIndex, (short) keyCount, isSync);
            }
            return 0;
        });
    }

    private record PvmRow(long keyHash, long expireAt, byte[] keyBytes, byte[] encoded) {
        @Override
        public String toString() {
            return "PvmRow{" +
                    "key=" + new String(keyBytes) +
                    ", keyHash=" + keyHash +
                    ", expireAt=" + expireAt +
                    '}';
        }
    }

    public void updatePvmListAfterWriteSegment(ArrayList<PersistValueMeta> pvmList) {
        callSync(() -> {
            var groupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(one -> one.bucketIndex));
            for (var entry : groupByBucketIndex.entrySet()) {
                var bucketIndex = entry.getKey();
                var list = entry.getValue();

                ArrayList<PvmRow> pvmRowList = new ArrayList<>(list.size());
                for (var pvm : list) {
                    pvmRowList.add(new PvmRow(pvm.keyHash, pvm.expireAt, pvm.keyBytes, pvm.encode()));
                }
                persistPvmListBatch(bucketIndex, pvmRowList);
            }
            return 0;
        });
    }

    private void updatePvmListAfterWriteSegmentBatchRead(ArrayList<PersistValueMeta> pvmList) {
        callSync(() -> {
            var groupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(one -> one.bucketIndex));
            // batch read
            var minBucketIndex = groupByBucketIndex.keySet().stream().min(Integer::compareTo).orElse(0);
            var maxBucketIndex = groupByBucketIndex.keySet().stream().max(Integer::compareTo).orElse(0);
            var diff = maxBucketIndex - minBucketIndex;
            if (diff > ConfForSlot.global.confWal.oneChargeBucketNumber) {
                throw new IllegalStateException("once update key buckets number > ConfForSlot.global.confWal.oneChargeBucketNumber");
            }

            int maxSplitNumber = 1;
            for (var entry : groupByBucketIndex.entrySet()) {
                var bucketIndex = entry.getKey();
                var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
                if (splitNumber > maxSplitNumber) {
                    maxSplitNumber = splitNumber;
                }
            }

            var isCompress = ConfForSlot.global.confBucket.isCompress;

            HashMap<Integer, ArrayList<KeyBucket>> keyBucketsByBucketIndex = new HashMap<>();
            for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
                var fdReadWrite = fdReadWriteArray[splitIndex];
                if (fdReadWrite == null) {
                    initFds((byte) (splitIndex + 1));
                    fdReadWrite = fdReadWriteArray[splitIndex];
                }

                var bytesBatch = fdReadWrite.readSegmentForKeyBucketWhenMergeCompare(minBucketIndex);
                if (bytesBatch == null) {
                    continue;
                }
                var bucketCount = bytesBatch.length / KEY_BUCKET_ONE_COST_SIZE;

                if (isCompress) {
                    for (int i = 0; i < bucketCount; i++) {
                        var bucketIndex = minBucketIndex + i;
                        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);

                        var bytes = new byte[KEY_BUCKET_ONE_COST_SIZE];
                        System.arraycopy(bytesBatch, i * KEY_BUCKET_ONE_COST_SIZE, bytes, 0, KEY_BUCKET_ONE_COST_SIZE);
                        if (!isBytesValidAsKeyBucket(bytes)) {
                            continue;
                        }

                        var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
                        keyBucket.initWithCompressStats(compressStats);

                        var list = keyBucketsByBucketIndex.computeIfAbsent(bucketIndex, k -> new ArrayList<>());
                        list.add(keyBucket);
                    }
                } else {
                    // share bytes
                    for (int i = 0; i < bucketCount; i++) {
                        var bucketIndex = minBucketIndex + i;
                        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);

                        var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytesBatch, i * KEY_BUCKET_ONE_COST_SIZE, snowFlake);
                        keyBucket.initWithCompressStats(compressStats);

                        var list = keyBucketsByBucketIndex.computeIfAbsent(bucketIndex, k -> new ArrayList<>());
                        list.add(keyBucket);
                    }
                }
            }

            for (var entry : groupByBucketIndex.entrySet()) {
                var bucketIndex = entry.getKey();
                var subPvmList = entry.getValue();

                ArrayList<PvmRow> pvmRowList = new ArrayList<>(subPvmList.size());
                for (var pvm : subPvmList) {
                    pvmRowList.add(new PvmRow(pvm.keyHash, pvm.expireAt, pvm.keyBytes, pvm.encode()));
                }

                var keyBucketList = keyBucketsByBucketIndex.get(bucketIndex);
                // todo
//                persistPvmListBatch2(bucketIndex, pvmRowList, keyBucketList);
            }
            return 0;
        });
    }

    private void persistPvmListBatch(int bucketIndex, ArrayList<PvmRow> pvmRowList) {
        updateBatch(bucketIndex, 0, (keyBuckets, putBackFlags, splitNumber, isLoadedAll) -> {
            var beforeKeyBuckets = new ArrayList<>(keyBuckets);
            byte[] beforeSplitNumberArr = new byte[]{splitNumber};

            for (int i = 0; i < pvmRowList.size(); i++) {
                var pvmRow = pvmRowList.get(i);
                int splitIndex = beforeSplitNumberArr[0] == 1 ? 0 : (int) Math.abs(pvmRow.keyHash() % beforeSplitNumberArr[0]);
                var keyBucket = beforeKeyBuckets.get(splitIndex);
                if (keyBucket == null) {
                    keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, null, snowFlake);
                    keyBucket.initWithCompressStats(compressStats);
                }

                boolean notSplit = beforeSplitNumberArr[0] == splitNumber;
                var afterPutKeyBuckets = notSplit ? new KeyBucket[SPLIT_MULTI_STEP] : null;

                if (keyBucket.isFull() && afterPutKeyBuckets == null) {
                    // stop this batch, do next time
                    var leftPvmRowList = new ArrayList<PvmRow>(pvmRowList.size() - i);
                    leftPvmRowList.addAll(pvmRowList.subList(i, pvmRowList.size()));
                    persistPvmListBatch(bucketIndex, leftPvmRowList);
                    break;
                }

                boolean isPut = keyBucket.put(pvmRow.keyBytes(), pvmRow.keyHash(), pvmRow.expireAt(), pvmRow.encoded(), afterPutKeyBuckets);
                if (!isPut) {
                    throw new IllegalStateException("Put pvm error, pvm: " + pvmRow);
                }

                splitOthersIfSplit(bucketIndex, keyBuckets, putBackFlags,
                        beforeKeyBuckets, beforeSplitNumberArr, splitIndex, keyBucket, notSplit, afterPutKeyBuckets);
            }
        }, true);
    }

    public void persistShortValueListBatch(int bucketIndex, List<Wal.V> shortValueList) {
        updateBatch(bucketIndex, 0, (keyBuckets, putBackFlags, splitNumber, isLoadedAll) -> {
            var beforeKeyBuckets = new ArrayList<>(keyBuckets);
            byte[] beforeSplitNumberArr = new byte[]{splitNumber};

            for (int i = 0; i < shortValueList.size(); i++) {
                var v = shortValueList.get(i);
                int splitIndex = beforeSplitNumberArr[0] == 1 ? 0 : (int) Math.abs(v.keyHash() % beforeSplitNumberArr[0]);
                var keyBucket = beforeKeyBuckets.get(splitIndex);

                boolean notSplit = beforeSplitNumberArr[0] == splitNumber;
                var afterPutKeyBuckets = notSplit ? new KeyBucket[SPLIT_MULTI_STEP] : null;

                // delete must not split
                if (v.isDeleteFlag()) {
                    if (keyBucket.del(v.key().getBytes(), v.keyHash()) && notSplit) {
                        putBackFlags[splitIndex] = true;
                    }
                    continue;
                }

                if (keyBucket.isFull() && afterPutKeyBuckets == null) {
                    // stop this batch, do next time
                    // why ? I forget why I write this... sign, need test, todo
                    var leftShortValueList = new ArrayList<Wal.V>(shortValueList.size() - i);
                    leftShortValueList.addAll(shortValueList.subList(i, shortValueList.size()));
                    persistShortValueListBatch(bucketIndex, leftShortValueList);
                    break;
                }

                boolean isPut = keyBucket.put(v.key().getBytes(), v.keyHash(), v.expireAt(), v.cvEncoded(), afterPutKeyBuckets);
                if (!isPut) {
                    throw new IllegalStateException("Put short value error, key: " + v.key());
                }

                splitOthersIfSplit(bucketIndex, keyBuckets, putBackFlags,
                        beforeKeyBuckets, beforeSplitNumberArr,
                        splitIndex, keyBucket, notSplit, afterPutKeyBuckets);
            }
        }, true);
    }

    private void splitOthersIfSplit(int bucketIndex, ArrayList<KeyBucket> keyBuckets, boolean[] putBackFlags,
                                    ArrayList<KeyBucket> beforeKeyBuckets, byte[] beforeSplitNumberArr,
                                    int splitIndex, KeyBucket keyBucket, boolean notSplit, KeyBucket[] afterPutKeyBuckets) {
        if (notSplit) {
            putBackFlags[splitIndex] = true;
        }

        if (afterPutKeyBuckets != null && afterPutKeyBuckets[0] != null) {
            // in write lock, stats need not thread safe here
            splitCount++;
            splitCostNanos += afterPutKeyBuckets[0].lastSplitCostNanos;

            // save all these
            for (var afterPutKeyBucket : afterPutKeyBuckets) {
                updateKeyBucketInner(bucketIndex, afterPutKeyBucket, true);
            }
            // already saved
            putBackFlags[splitIndex] = false;

            beforeKeyBuckets.clear();
            beforeKeyBuckets.addAll(Arrays.asList(afterPutKeyBuckets));

            // other key bucket also need split
            for (var kb : keyBuckets) {
                if (kb == keyBucket) {
                    continue;
                }

                var kbArray = kb.split();
                for (var bucket : kbArray) {
                    updateKeyBucketInner(bucketIndex, bucket, true);
                    beforeKeyBuckets.add(bucket);
                }
            }

            if (beforeKeyBuckets.size() != keyBuckets.size() * SPLIT_MULTI_STEP) {
                throw new IllegalStateException("After split key bucket size not match, size: " + beforeKeyBuckets.size() +
                        ", before size: " + keyBuckets.size() + ", split multi step: " + SPLIT_MULTI_STEP);
            }

            // sort by split index
            beforeKeyBuckets.sort(Comparator.comparingInt(kb -> kb.splitIndex));

            if (afterPutKeyBuckets[0].splitNumber == beforeSplitNumberArr[0]) {
                throw new IllegalStateException("Split number not changed after split, split number: "
                        + afterPutKeyBuckets[0].splitNumber);
            }
            beforeSplitNumberArr[0] = afterPutKeyBuckets[0].splitNumber;

            // update meta
            metaKeyBucketSplitNumber.set(bucketIndex, beforeSplitNumberArr[0]);
            masterUpdateCallback.onKeyBucketSplit(slot, bucketIndex, beforeSplitNumberArr[0]);
        }
    }

    public boolean remove(int bucketIndex, byte[] keyBytes, long keyHash) {
        boolean[] deleteFlags = new boolean[1];
        updateBatch(bucketIndex, keyHash, (keyBuckets, putFlags, splitNumber, isLoadedAll) -> {
            // key hash is not 0, just get one target key bucket
            var keyBucket = keyBuckets.get(0);
            if (keyBucket.size == 0) {
                return;
            }

            boolean isDel = keyBucket.del(keyBytes, keyHash);
            if (isDel) {
                putFlags[0] = true;
            }
            deleteFlags[0] = isDel;
        }, false);
        return deleteFlags[0];
    }

    public void flush() {
        callSync(() -> {
            metaKeyBucketSplitNumber.clear();
            statKeyBucketLastUpdateCount.clear();

            boolean[] ftruncateFlags = new boolean[MAX_SPLIT_NUMBER];

            for (int i = 0; i < bucketsPerSlot; i++) {
                for (int splitIndex = 0; splitIndex < MAX_SPLIT_NUMBER; splitIndex++) {
                    var fdReadWrite = fdReadWriteArray[splitIndex];
                    if (fdReadWrite == null) {
                        continue;
                    }

                    if (ftruncateFlags[splitIndex]) {
                        continue;
                    }

                    fdReadWrite.truncate();
                    ftruncateFlags[splitIndex] = true;
                }
            }
            return 0;
        });
    }

    private final SimpleGauge keyLoaderInnerGauge = new SimpleGauge("key_loader_inner", "key loader inner",
            "slot");

    private void initMetricsCollect() {
        keyLoaderInnerGauge.register();

        keyLoaderInnerGauge.setRawGetter(() -> {
            var labelValues = List.of(slotStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("bucket_count", new SimpleGauge.ValueWithLabelValues((double) bucketsPerSlot, labelValues));
            map.put("persist_key_count", new SimpleGauge.ValueWithLabelValues((double) getKeyCount(), labelValues));
            map.put("loaded_key_count", new SimpleGauge.ValueWithLabelValues((double) compressStats.getAllTmpBucketSize(), labelValues));

            map.put("split_count", new SimpleGauge.ValueWithLabelValues((double) splitCount, labelValues));
            if (splitCount > 0) {
                map.put("split_cost_avg_micros", new SimpleGauge.ValueWithLabelValues((double) splitCostNanos / splitCount / 1000, labelValues));
            }

            var stats = compressStats.stats();
            for (var stat : stats) {
                if (stat == StatKV.split) {
                    continue;
                }
                map.put(stat.key().replaceAll(" ", "_"),
                        new SimpleGauge.ValueWithLabelValues(stat.value(), labelValues));
            }
            return map;
        });
    }
}
