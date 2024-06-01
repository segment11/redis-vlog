package redis.tools;

import redis.ConfForSlot;
import redis.persist.KeyBucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class IterateKeyBuckets {
    public static void main(String[] args) throws IOException {
        byte slot = 0;
        byte splitIndex = 1;
        byte splitNumber = 8;

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
                System.out.println("size: " + size + ", bucket index: " + i);
                var keyBucket = new KeyBucket((byte) 0, i, splitIndex, splitNumber, bytes, null);
                int finalI = i;
                keyBucket.iterate((keyHash, expireAt, seq, keyBytes, valueBytes) -> {
                    System.out.println("key: " + new String(keyBytes) + ", key hash: " + keyHash);

                    var bucketIndexExpect = Math.abs(keyHash % bucketsPerSlot);
                    if (bucketIndexExpect != finalI) {
                        System.out.println("bucket index expect: " + bucketIndexExpect + ", bucket index: " + finalI);
                    }
                });
            }

            i++;
        }
        fis.close();

        for (int j = 0; j < sumArray.length; j++) {
            System.out.print(sumArray[j] + " ");
            if (j % 1024 == 0) {
                System.out.println();
            }
        }
        System.out.println();
    }
}
