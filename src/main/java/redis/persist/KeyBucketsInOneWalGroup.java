package redis.persist;

import redis.ConfForSlot;

import java.util.ArrayList;
import java.util.Collection;

public class KeyBucketsInOneWalGroup {
    public KeyBucketsInOneWalGroup(byte slot, int groupIndex, KeyLoader keyLoader) {
        this.slot = slot;
        this.groupIndex = groupIndex;
        this.keyLoader = keyLoader;

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        this.oneChargeBucketNumber = oneChargeBucketNumber;
        this.splitNumberTmp = new byte[oneChargeBucketNumber];
        this.beginBucketIndex = oneChargeBucketNumber * groupIndex;

        this.emptyBytes = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE];
    }

    private final byte slot;
    private final int groupIndex;
    private final int oneChargeBucketNumber;
    // index is bucket index - begin bucket index
    final byte[] splitNumberTmp;
    final int beginBucketIndex;

    private final KeyLoader keyLoader;

    // outer index is split index, inner index is bucket index - begin bucket index
    private ArrayList<ArrayList<KeyBucket>> listList = new ArrayList<>();

    boolean isBucketIndexInThisWalGroup(int bucketIndex) {
        return bucketIndex >= beginBucketIndex && bucketIndex < beginBucketIndex + oneChargeBucketNumber;
    }

    KeyBucket getKeyBucket(int bucketIndex, byte splitIndex, byte splitNumber, long keyHash) {
        var currentSplitNumber = splitNumberTmp[bucketIndex - beginBucketIndex];
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

    void readBeforePutBatch() {
        var maxSplitNumber = keyLoader.maxSplitNumber();
        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            if (listList.size() <= splitIndex) {
                listList.add(null);
            }
        }

        for (int i = 0; i < oneChargeBucketNumber; i++) {
            var bucketIndex = beginBucketIndex + i;
            var splitNumber = keyLoader.getKeyBucketSplitNumber(bucketIndex);
            splitNumberTmp[i] = splitNumber;
        }

        for (int splitIndex = 0; splitIndex < maxSplitNumber; splitIndex++) {
            var list = new ArrayList<KeyBucket>();
            for (int i = 0; i < oneChargeBucketNumber; i++) {
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
                var bucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, currentSplitNumber, sharedBytes, KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i, keyLoader.snowFlake);
                list.set(i, bucket);
            }
        }
    }

    private final byte[] emptyBytes;

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
                var lastWriteOffset = 0;
                for (int i = 0; i < oneChargeBucketNumber; i++) {
                    int destPos = KeyLoader.KEY_BUCKET_ONE_COST_SIZE * i;
                    if (lastWriteOffset > destPos) {
                        continue;
                    }

                    var keyBucket = list.get(i);
                    if (keyBucket == null) {
                        System.arraycopy(emptyBytes, 0, sharedBytes, destPos, KeyLoader.KEY_BUCKET_ONE_COST_SIZE);
                        lastWriteOffset += KeyLoader.KEY_BUCKET_ONE_COST_SIZE;
                    } else {
                        System.arraycopy(keyBucket.bytes, 0, sharedBytes, destPos, keyBucket.bytes.length);
                        lastWriteOffset += keyBucket.bytes.length;
                    }
                }
            }

            sharedBytesList[splitIndex] = sharedBytes;
        }
        return sharedBytesList;
    }

    boolean isSplit = false;

    void putAllPvmList(Collection<PersistValueMeta> pvmList) {
        for (var pvm : pvmList) {
            int bucketIndex = pvm.bucketIndex;
            int relativeBucketIndex = bucketIndex - beginBucketIndex;

            var currentSplitNumber = splitNumberTmp[relativeBucketIndex];
            var splitIndex = currentSplitNumber == 1 ? 0 : (int) Math.abs(pvm.keyHash % currentSplitNumber);

            var afterPutKeyBuckets = currentSplitNumber == KeyLoader.MAX_SPLIT_NUMBER ? null : new KeyBucket[currentSplitNumber * KeyLoader.SPLIT_MULTI_STEP];

            var list = listList.get(splitIndex);
            var keyBucket = list.get(relativeBucketIndex);
            if (keyBucket == null) {
                keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, currentSplitNumber, null, 0, keyLoader.snowFlake);
                list.set(relativeBucketIndex, keyBucket);
            }

            boolean isPut = keyBucket.put(pvm.keyBytes, pvm.keyHash, pvm.expireAt, pvm.seq,
                    pvm.extendBytes != null ? pvm.extendBytes : pvm.encode(), afterPutKeyBuckets);
            if (!isPut) {
                throw new RuntimeException("Key buckets put batch short value list failed");
            }
            if (afterPutKeyBuckets[0] != null) {
                isSplit = true;
                splitNumberTmp[relativeBucketIndex] = (byte) afterPutKeyBuckets.length;

                if (listList.size() < afterPutKeyBuckets.length) {
                    for (int i = listList.size(); i < afterPutKeyBuckets.length; i++) {
                        var listTmp = new ArrayList<KeyBucket>();
                        for (int j = 0; j < oneChargeBucketNumber; j++) {
                            listTmp.add(null);
                        }
                        listList.add(listTmp);
                    }
                }

                for (int i = 0; i < afterPutKeyBuckets.length; i++) {
                    var listTmp = listList.get(i);
                    listTmp.set(relativeBucketIndex, afterPutKeyBuckets[i]);
                }
            }
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
        putAllPvmList(pvmList);
    }
}
