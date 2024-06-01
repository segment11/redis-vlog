package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class MetaKeyBucketSplitNumber {
    private static final String META_KEY_BUCKET_SPLIT_NUMBER_FILE = "meta_key_bucket_split_number.dat";

    private final byte slot;
    private final int allCapacity;
    private RandomAccessFile raf;

    private final byte[] inMemoryCachedBytes;

    private final ByteBuffer inMemoryCachedByteBuffer;

    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl meta key bucket split number, bytes length not match");
        }

        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.position(0).put(bytes);
            return;
        }

        try {
            raf.seek(0);
            raf.write(bytes);
            inMemoryCachedByteBuffer.position(0).put(bytes);
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
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
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

            var sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                sb.append(inMemoryCachedBytes[i]).append(", ");
            }
            log.info("For debug: first 10 key bucket split number: [{}]", sb);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    // for unit test
    void setForTest(int bucketIndex, byte splitNumber) {
        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.put(bucketIndex, splitNumber);
            return;
        }

        var offset = bucketIndex;
        try {
            raf.seek(offset);
            raf.writeByte(splitNumber);
            inMemoryCachedByteBuffer.put(bucketIndex, splitNumber);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void setBatch(int beginBucketIndex, byte[] splitNumberArray) {
        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.position(beginBucketIndex).put(splitNumberArray);
            return;
        }

        var offset = beginBucketIndex;
        try {
            raf.seek(offset);
            raf.write(splitNumberArray);
            inMemoryCachedByteBuffer.position(beginBucketIndex).put(splitNumberArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    byte[] getBatch(int beginBucketIndex, int bucketCount) {
        var dst = new byte[bucketCount];
        inMemoryCachedByteBuffer.position(beginBucketIndex).get(dst);
        return dst;
    }

    byte get(int bucketIndex) {
        return inMemoryCachedByteBuffer.get(bucketIndex);
    }

    byte maxSplitNumber() {
        byte max = 1;
        for (int j = 0; j < allCapacity; j++) {
            var splitNumber = inMemoryCachedByteBuffer.get(j);
            if (splitNumber > max) {
                max = splitNumber;
            }
        }
        return max;
    }

    void clear() {
        if (ConfForSlot.global.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 1);
            inMemoryCachedByteBuffer.position(0).put(inMemoryCachedBytes);
            return;
        }

        try {
            Arrays.fill(inMemoryCachedBytes, (byte) 1);
            raf.seek(0);
            raf.write(inMemoryCachedBytes);
            inMemoryCachedByteBuffer.position(0).put(inMemoryCachedBytes);
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
//            raf.getFD().sync();
//            System.out.println("Meta key bucket split number sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
