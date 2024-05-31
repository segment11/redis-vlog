package redis.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.ConfForSlot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class KeyBucketsInOneWalGroup {
    public KeyBucketsInOneWalGroup(byte slot, int groupIndex, KeyLoader keyLoader) {
        this.slot = slot;
        this.keyLoader = keyLoader;

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        this.oneChargeBucketNumber = oneChargeBucketNumber;
        this.keyCountTmp = new int[oneChargeBucketNumber];
        this.beginBucketIndex = oneChargeBucketNumber * groupIndex;

        this.readBeforePutBatch();
    }

    private final byte slot;
    private final int oneChargeBucketNumber;
    // index is bucket index - begin bucket index
    byte[] splitNumberTmp;
    final int[] keyCountTmp;
    final int beginBucketIndex;

    private final KeyLoader keyLoader;

    private final Logger log = LoggerFactory.getLogger(KeyBucketsInOneWalGroup.class);

    // outer index is split index, inner index is bucket index - begin bucket index
    private ArrayList<ArrayList<KeyBucket>> listList = new ArrayList<>();

    KeyBucket getKeyBucket(int bucketIndex, byte splitIndex, byte splitNumber, long keyHash) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];
        if (currentSplitNumber == splitNumber) {
            var list = listList.get(splitIndex);
            if (list == null) {
                return null;
            }
            return list.get(bucketIndex - beginBucketIndex);
        } else {
            // calc split index again
            var splitIndexTmp = currentSplitNumber == 1 ? 0 : (int) Math.abs(keyHash % currentSplitNumber);
            var list = listList.get(splitIndexTmp);
            if (list == null) {
                return null;
            }
            return list.get(bucketIndex - beginBucketIndex);
        }
    }

    private void readBeforePutBatch() {
        this.splitNumberTmp = keyLoader.getMetaKeyBucketSplitNumberBatch(beginBucketIndex, oneChargeBucketNumber);
        int maxSplitNumber = 0;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            if (splitNumberTmp[i] > maxSplitNumber) {
                maxSplitNumber = splitNumberTmp[i];
            }
        }

        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            if (listList.size() <= splitIndex) {
                // init size with null
                listList.add(null);
            }
        }

        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            var list = new ArrayList<KeyBucket>();
            for (int i = 0; i < oneChargeBucketNumber; i++) {
                // init size with null
                list.add(null);
            }
            listList.set(splitIndex, list);

            var sharedBytes = keyLoader.readBatchInOneWalGroup((byte) splitIndex, beginBucketIndex);
            if (sharedBytes == null) {
                continue;
            }

            for (int i = 0; i < oneChargeBucketNumber; i++) {
                var bucketIndex = beginBucketIndex + i;
                var currentSplitNumber = splitNumberTmp[i];
                var bucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, currentSplitNumber, sharedBytes,
                        KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i, keyLoader.snowFlake);
                keyCountTmp[i] += bucket.size;
                list.set(i, bucket);
            }
        }
    }

    KeyBucket.ValueBytesWithExpireAtAndSeq getValue(int bucketIndex, byte[] keyBytes, long keyHash) {
        var currentSplitNumber = splitNumberTmp[bucketIndex - beginBucketIndex];
        var splitIndex = currentSplitNumber == 1 ? 0 : (int) Math.abs(keyHash % currentSplitNumber);
        var keyBucket = getKeyBucket(bucketIndex, (byte) splitIndex, currentSplitNumber, keyHash);
        if (keyBucket == null) {
            return null;
        }

        return keyBucket.getValueByKey(keyBytes, keyHash);
    }

    private final byte[] EMPTY_BYTES = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE];

    byte[][] writeAfterPutBatch() {
        int maxSplitNumberTmp = 0;
        for (int i = 0; i < oneChargeBucketNumber; i++) {
            if (splitNumberTmp[i] > maxSplitNumberTmp) {
                maxSplitNumberTmp = splitNumberTmp[i];
            }
        }

        var sharedBytesList = new byte[maxSplitNumberTmp][];

        for (int splitIndex = 0; splitIndex < listList.size(); splitIndex++) {
            var list = listList.get(splitIndex);
            for (var keyBucket : list) {
                if (keyBucket != null) {
                    keyBucket.encode();
                }
            }

            var isAllSharedBytes = true;
            for (var keyBucket : list) {
                if (keyBucket == null || !keyBucket.isSharedBytes()) {
                    isAllSharedBytes = false;
                    break;
                }
            }

            byte[] sharedBytes;
            if (isAllSharedBytes) {
                sharedBytes = list.get(0).bytes;
            } else {
                sharedBytes = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE * oneChargeBucketNumber];
                for (int i = 0; i < oneChargeBucketNumber; i++) {
                    int destPos = KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i;

                    var keyBucket = list.get(i);
                    if (keyBucket == null) {
                        System.arraycopy(EMPTY_BYTES, 0, sharedBytes, destPos, KeyLoader.KEY_BUCKET_ONE_COST_SIZE);
                    } else {
                        System.arraycopy(keyBucket.bytes, keyBucket.position, sharedBytes, destPos, KeyLoader.KEY_BUCKET_ONE_COST_SIZE);
                    }
                }
            }

            sharedBytesList[splitIndex] = sharedBytes;
        }
        return sharedBytesList;
    }

    boolean isSplit = false;

    private void putPvmListToTargetBucketAfterClearAll(List<PersistValueMeta> needAddNewList, List<PersistValueMeta> needDeleteList, Integer bucketIndex) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];

        for (var pvm : needAddNewList) {
            var splitIndex = currentSplitNumber == 1 ? 0 : (int) Math.abs(pvm.keyHash % currentSplitNumber);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, currentSplitNumber, null, 0, keyLoader.snowFlake);
                list.set(relativeBucketIndex, keyBucket);
            }

            var doPutResult = keyBucket.put(pvm.keyBytes, pvm.keyHash, pvm.expireAt, pvm.seq,
                    pvm.extendBytes != null ? pvm.extendBytes : pvm.encode(), null);
            if (!doPutResult.isUpdate()) {
                keyCountTmp[relativeBucketIndex]++;
            }
        }

        for (var pvm : needDeleteList) {
            var splitIndex = currentSplitNumber == 1 ? 0 : (int) Math.abs(pvm.keyHash % currentSplitNumber);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                continue;
            }

            var isDeleted = keyBucket.del(pvm.keyBytes, pvm.keyHash, true);
            if (isDeleted) {
                keyCountTmp[relativeBucketIndex]--;
            }
        }
    }

    private void putPvmListToTargetBucket(List<PersistValueMeta> pvmListThisBucket, Integer bucketIndex, boolean isMerge) {
        int relativeBucketIndex = bucketIndex - beginBucketIndex;
        var currentSplitNumber = splitNumberTmp[relativeBucketIndex];

        List<PersistValueMeta> needAddNewList = new ArrayList<>();
        List<PersistValueMeta> needDeleteList = new ArrayList<>();
        List<PersistValueMeta> needUpdateList = new ArrayList<>();

//        int currentTotalKeyCountThisBucketFromStats = keyCountTmp[relativeBucketIndex];
        int currentTotalKeyCountThisBucket = 0;
        int currentTotalCellCostThisBucket = 0;
        int[] existsKeyCountBySplitIndex = new int[currentSplitNumber];
        int[] existsCellCostBySplitIndex = new int[currentSplitNumber];
        for (int splitIndex = 0; splitIndex < currentSplitNumber; splitIndex++) {
            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                continue;
            }

            keyBucket.clearAllExpired();
            currentTotalKeyCountThisBucket += keyBucket.size;
            currentTotalCellCostThisBucket += keyBucket.cellCost;

            existsKeyCountBySplitIndex[splitIndex] = keyBucket.size;
            existsCellCostBySplitIndex[splitIndex] = keyBucket.cellCost;
        }

        int[] needAddKeyCountBySplitIndex = new int[currentSplitNumber];
        int[] needAddCellCostBySplitIndex = new int[currentSplitNumber];

        for (var pvm : pvmListThisBucket) {
            var splitIndex = currentSplitNumber == 1 ? 0 : (int) Math.abs(pvm.keyHash % currentSplitNumber);

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                if (!isMerge) {
                    needAddNewList.add(pvm);

                    needAddKeyCountBySplitIndex[splitIndex]++;
                    int valueLength = pvm.extendBytes != null ? pvm.extendBytes.length : PersistValueMeta.ENCODED_LEN;
                    needAddCellCostBySplitIndex[splitIndex] += KeyBucket.KVMeta.calcCellCount((short) pvm.keyBytes.length, (byte) valueLength);
                }
                continue;
            }

            var currentOne = keyBucket.getValueByKey(pvm.keyBytes, pvm.keyHash);
            if (isMerge) {
                // only put if seq match, as between merge worker compare and persist, the value may be updated
                if (currentOne == null) {
                    // already removed
                    continue;
                }

                if (currentOne.seq() != pvm.seq) {
                    // already updated
                    continue;
                }
            }

            // wal remove delay use expire now
            if (pvm.expireAt == CompressedValue.EXPIRE_NOW) {
                if (currentOne != null) {
                    needDeleteList.add(pvm);
                }
                continue;
            }

            if (currentOne == null) {
                // not exists
                needAddNewList.add(pvm);

                needAddKeyCountBySplitIndex[splitIndex]++;
                int valueLength = pvm.extendBytes != null ? pvm.extendBytes.length : PersistValueMeta.ENCODED_LEN;
                needAddCellCostBySplitIndex[splitIndex] += KeyBucket.KVMeta.calcCellCount((short) pvm.keyBytes.length, (byte) valueLength);
            } else {
                needUpdateList.add(pvm);
            }
        }

        var currentMaxSplitNumber = listList.size();
        var canPutKeyCountThisBucket = KeyBucket.INIT_CAPACITY * currentMaxSplitNumber;

        var newKeyCountNeedThisBucket = currentTotalKeyCountThisBucket + needAddNewList.size() - needDeleteList.size();
        int newCellCostNeedThisBucket = currentTotalCellCostThisBucket;
        for (var pvm : needAddNewList) {
            var valueLength = pvm.extendBytes != null ? pvm.extendBytes.length : PersistValueMeta.ENCODED_LEN;
            newCellCostNeedThisBucket += KeyBucket.KVMeta.calcCellCount((short) pvm.keyBytes.length, (byte) valueLength);
        }

        // todo, change here
        final int tolerance = 2;

        // split
        var needSplit = false;
        if (newKeyCountNeedThisBucket > canPutKeyCountThisBucket - tolerance) {
            needSplit = true;
        } else if (newCellCostNeedThisBucket > canPutKeyCountThisBucket - tolerance) {
            needSplit = true;
        }

        if (!needSplit) {
            // compare each split index
            for (int splitIndex = 0; splitIndex < currentSplitNumber; splitIndex++) {
                var existsKeyCount = existsKeyCountBySplitIndex[splitIndex];
                var needAddKeyCount = needAddKeyCountBySplitIndex[splitIndex];

                if (existsKeyCount + needAddKeyCount > KeyBucket.INIT_CAPACITY - tolerance) {
                    needSplit = true;
                    break;
                }

                var existsCellCost = existsCellCostBySplitIndex[splitIndex];
                var needAddCellCost = needAddCellCostBySplitIndex[splitIndex];
                if (existsCellCost + needAddCellCost > KeyBucket.INIT_CAPACITY - tolerance) {
                    needSplit = true;
                    break;
                }
            }
        }

        if (needSplit) {
            var newMaxSplitNumber = currentMaxSplitNumber * KeyLoader.SPLIT_MULTI_STEP;
            if (newMaxSplitNumber > KeyLoader.MAX_SPLIT_NUMBER) {
                log.warn("Bucket full, split number exceed max split number: " + KeyLoader.MAX_SPLIT_NUMBER + ", slot: " + slot + ", bucketIndex: " + bucketIndex);
                // log all keys
                log.warn("Failed keys to put: {}", pvmListThisBucket.stream().map(pvm -> new String(pvm.keyBytes)).collect(Collectors.toList()));
                throw new BucketFullException("Bucket full, split number exceed max split number: " + KeyLoader.MAX_SPLIT_NUMBER);
            }

            for (int i = currentMaxSplitNumber; i < newMaxSplitNumber; i++) {
                var listTmp = new ArrayList<KeyBucket>();
                for (int j = 0; j < oneChargeBucketNumber; j++) {
                    listTmp.add(null);
                }
                listList.add(listTmp);
            }
            splitNumberTmp[relativeBucketIndex] = (byte) newMaxSplitNumber;

            // rehash
            List<PersistValueMeta> existsWithoutNeedUpdatePvmList = new ArrayList<>();
            for (var list : listList) {
                var keyBucket = list.get(relativeBucketIndex);
                if (keyBucket == null) {
                    continue;
                }

                keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
                    if (!needUpdateList.isEmpty()) {
                        for (var needUpdatePvm : needUpdateList) {
                            if (needUpdatePvm.keyHash == keyHash && Arrays.equals(needUpdatePvm.keyBytes, keyBytes)) {
                                return;
                            }
                        }
                    }

                    var pvm = new PersistValueMeta();
                    pvm.expireAt = expireAt;
                    pvm.seq = seq;
                    pvm.keyBytes = keyBytes;
                    pvm.keyHash = keyHash;
                    pvm.bucketIndex = bucketIndex;
                    pvm.extendBytes = valueBytes;
                    existsWithoutNeedUpdatePvmList.add(pvm);
                });
            }

            needAddNewList.addAll(needUpdateList);
            needAddNewList.addAll(existsWithoutNeedUpdatePvmList);

            for (var list : listList) {
                var keyBucket = list.get(relativeBucketIndex);
                if (keyBucket != null) {
                    keyBucket.clearAll();
                }
            }
            keyCountTmp[relativeBucketIndex] = 0;

            isSplit = true;
        } else {
            needAddNewList.addAll(needUpdateList);
        }

        putPvmListToTargetBucketAfterClearAll(needAddNewList, needDeleteList, bucketIndex);
    }

    void putAllPvmList(ArrayList<PersistValueMeta> pvmList, boolean isMerge) {
        // group by bucket index
        var pvmListGroupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(pvm -> pvm.bucketIndex));
        for (var entry : pvmListGroupByBucketIndex.entrySet()) {
            var bucketIndex = entry.getKey();
            var pvmListThisBucket = entry.getValue();

            putPvmListToTargetBucket(pvmListThisBucket, bucketIndex, isMerge);
        }
    }

    void putAll(Collection<Wal.V> shortValueList) {
        var pvmList = new ArrayList<PersistValueMeta>();
        for (var v : shortValueList) {
            var pvm = new PersistValueMeta();
            pvm.expireAt = v.expireAt();
            pvm.seq = v.seq();
            pvm.keyBytes = v.key().getBytes();
            pvm.keyHash = v.keyHash();
            pvm.bucketIndex = v.bucketIndex();
            pvm.extendBytes = v.cvEncoded();
            pvmList.add(pvm);
        }
        putAllPvmList(pvmList, false);
    }
}
