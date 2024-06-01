package redis.tools;

import redis.ConfForSlot;
import redis.persist.KeyBucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IterateKeyBuckets {
    public static void main(String[] args) throws IOException {
        byte slot = 0;
        byte splitIndex = 0;
        byte splitNumber = 8;

        for (int i = 0; i < splitNumber; i++) {
            iterateOneSplitIndex(slot, (byte) i, splitNumber);
        }

        // get size > 100
        for (var entry : keysByBucketIndex.entrySet()) {
            var bucketIndex = entry.getKey();
            Set<String> keys = entry.getValue();
            if (keys.size() > 46) {
                System.out.println("bucket index: " + bucketIndex + ", size: " + keys.size() + ", keys: " + keys);
            }

            if (bucketIndex == 7315) {
                System.out.println("bucket index: " + bucketIndex + ", size: " + keys.size() + ", keys: " + keys);
            }
        }
    }

    private static Map<Integer, Set<String>> keysByBucketIndex = new HashMap<>();

    public static void iterateOneSplitIndex(byte slot, byte splitIndex, byte splitNumber) throws IOException {
        var bucketsPerSlot = ConfForSlot.ConfBucket.c10m.bucketsPerSlot;
        int[] sumArray = new int[bucketsPerSlot];

        var slotDir = new File("/tmp/redis-vlog/persist/slot-" + slot);
        var splitKeyBucketsFile = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat");
        if (!splitKeyBucketsFile.exists()) {
            return;
        }

        var bytes = new byte[4096];
        var fis = new FileInputStream(splitKeyBucketsFile);
        int i = 0;
        while (fis.read(bytes) != -1) {
            var size = ByteBuffer.wrap(bytes, 8, 2).getShort();
            sumArray[i] += size;
            if (size > 0) {
                var set = keysByBucketIndex.get(i);
                if (set == null) {
                    set = new java.util.HashSet<>();
                    keysByBucketIndex.put(i, set);
                }

//                System.out.println("size: " + size + ", bucket index: " + i);
                var keyBucket = new KeyBucket((byte) 0, i, splitIndex, splitNumber, bytes, null);
//                int finalI = i;
                Set<String> finalSet = set;
                keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
//                    System.out.println("key: " + new String(keyBytes) + ", key hash: " + keyHash);

//                    var bucketIndexExpect = Math.abs(keyHash % bucketsPerSlot);
//                    if (bucketIndexExpect != finalI) {
//                        System.out.println("bucket index expect: " + bucketIndexExpect + ", bucket index: " + finalI);
//                    }
                    finalSet.add(new String(keyBytes));
                });
            }

            i++;
        }
        fis.close();

//        for (int j = 0; j < sumArray.length; j++) {
//            System.out.print(sumArray[j] + " ");
//            if (j % 1024 == 0) {
//                System.out.println();
//            }
//        }
//        System.out.println();
    }
}
