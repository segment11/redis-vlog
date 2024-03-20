package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class MetaKeyBucketSplitNumber {
    private static final String META_KEY_BUCKET_SPLIT_NUMBER_FILE = "meta_key_bucket_split_number.dat";

    private final byte slot;
    private final int allCapacity;
    private final RandomAccessFile raf;

    // 64KB
    private static final int BATCH_SIZE = 1024 * 64;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    private final byte[] inMemoryCachedBytes;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaKeyBucketSplitNumber(byte slot, int bucketsPerSlot, File slotDir) throws IOException {
        this.slot = slot;
        this.allCapacity = bucketsPerSlot;

        // max 512KB
        this.inMemoryCachedBytes = new byte[allCapacity];

        boolean needRead = false;
        var file = new File(slotDir, META_KEY_BUCKET_SPLIT_NUMBER_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);

            var initTimes = allCapacity / BATCH_SIZE;
            if (allCapacity % BATCH_SIZE != 0) {
                initTimes++;
            }
            for (int i = 0; i < initTimes; i++) {
                FileUtils.writeByteArrayToFile(file, EMPTY_BYTES, true);
            }
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

    public synchronized void set(int bucketIndex, byte splitNumber) {
        var offset = bucketIndex;
        try {
            raf.seek(offset);
            raf.writeByte(splitNumber);
            inMemoryCachedBytes[bucketIndex] = splitNumber;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized byte get(int bucketIndex) {
        return inMemoryCachedBytes[bucketIndex];
    }

    public synchronized byte maxSplitNumber() {
        byte max = 1;
        for (int j = 0; j < allCapacity; j++) {
            var splitNumber = inMemoryCachedBytes[j];
            if (splitNumber > max) {
                max = splitNumber;
            }
        }
        return max;
    }

    public synchronized void clear() {
        var initTimes = allCapacity / BATCH_SIZE;
        if (allCapacity % BATCH_SIZE != 0) {
            initTimes++;
        }
        try {
            for (int i = 0; i < initTimes; i++) {
                raf.seek((long) i * BATCH_SIZE);
                raf.write(EMPTY_BYTES);
            }
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void cleanUp() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
