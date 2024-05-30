package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class MetaKeyBucketSplitNumber {
    private static final String META_KEY_BUCKET_SPLIT_NUMBER_FILE = "meta_key_bucket_split_number.dat";

    private final byte slot;
    private final int allCapacity;
    private RandomAccessFile raf;

    private final byte[] inMemoryCachedBytes;

    byte[] getInMemoryCachedBytes() {
        return inMemoryCachedBytes;
    }

    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl meta key bucket split number, bytes length not match");
        }
        System.arraycopy(bytes, 0, inMemoryCachedBytes, 0, bytes.length);

        if (ConfForSlot.global.pureMemory) {
            return;
        }

        try {
            raf.seek(0);
            raf.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Repl meta key bucket split number, write file error", e);
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaKeyBucketSplitNumber(byte slot, File slotDir) throws IOException {
        this.slot = slot;

        this.allCapacity = ConfForSlot.global.confBucket.bucketsPerSlot;

        // max 512KB
        this.inMemoryCachedBytes = new byte[allCapacity];
        Arrays.fill(inMemoryCachedBytes, (byte) 1);

        if (ConfForSlot.global.pureMemory) {
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_KEY_BUCKET_SPLIT_NUMBER_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);
            FileUtils.writeByteArrayToFile(file, this.inMemoryCachedBytes, true);
        } else {
            needRead = true;
        }
        this.raf = new RandomAccessFile(file, "rw");

        if (needRead) {
            raf.seek(0);
            raf.read(inMemoryCachedBytes);
            log.warn("Read meta key bucket split number file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }
    }

    // read write lock better
    void set(int bucketIndex, byte splitNumber) {
        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedBytes[bucketIndex] = splitNumber;
            return;
        }

        var offset = bucketIndex;
        try {
            raf.seek(offset);
            raf.writeByte(splitNumber);
            inMemoryCachedBytes[bucketIndex] = splitNumber;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void setBatch(int beginBucketIndex, byte[] splitNumberArray) {
        if (ConfForSlot.global.pureMemory) {
            System.arraycopy(splitNumberArray, 0, inMemoryCachedBytes, beginBucketIndex, splitNumberArray.length);
            return;
        }

        var offset = beginBucketIndex;
        try {
            raf.seek(offset);
            raf.write(splitNumberArray);
            System.arraycopy(splitNumberArray, 0, inMemoryCachedBytes, beginBucketIndex, splitNumberArray.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    byte[] getBatch(int beginBucketIndex, int bucketCount) {
        return Arrays.copyOfRange(inMemoryCachedBytes, beginBucketIndex, beginBucketIndex + bucketCount);
    }

    byte get(int bucketIndex) {
        return inMemoryCachedBytes[bucketIndex];
    }

    byte maxSplitNumber() {
        byte max = 1;
        for (int j = 0; j < allCapacity; j++) {
            var splitNumber = inMemoryCachedBytes[j];
            if (splitNumber > max) {
                max = splitNumber;
            }
        }
        return max;
    }

    void clear() {
        if (ConfForSlot.global.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            return;
        }

        try {
            raf.seek(0);
            raf.write(inMemoryCachedBytes);
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanUp() {
        if (ConfForSlot.global.pureMemory) {
            return;
        }

        // sync all
        try {
            raf.getFD().sync();
            System.out.println("Meta key bucket split number sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
