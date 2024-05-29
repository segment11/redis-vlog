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

public class StatKeyCountInBuckets {
    private static final String STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE = "stat_key_count_in_buckets.dat";
    // short is enough for one key bucket total value count
    public static final int ONE_LENGTH = 2;

    private final byte slot;
    private final int bucketsPerSlot;
    private final int allCapacity;
    private RandomAccessFile raf;

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public StatKeyCountInBuckets(byte slot, int bucketsPerSlot, File slotDir) throws IOException {
        this.slot = slot;
        this.bucketsPerSlot = bucketsPerSlot;
        this.allCapacity = bucketsPerSlot * ONE_LENGTH;

        // max 512KB * 2 = 1MB
        this.inMemoryCachedBytes = new byte[allCapacity];

        if (ConfForSlot.global.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE);
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
            log.warn("Read stat key count in buckets file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    interface IterateCallBack {
        void call(byte slot, int bucketIndex, short size);
    }

    void iterate(IterateCallBack callBack) {
        for (int bucketIndex = 0; bucketIndex < bucketsPerSlot; bucketIndex++) {
            var size = inMemoryCachedByteBuffer.getShort(bucketIndex * ONE_LENGTH);
            callBack.call(slot, bucketIndex, size);
        }
    }

    void setKeyCountForBucketIndex(int bucketIndex, short keyCount) {
        var offset = bucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.putShort(offset, keyCount);
    }

    short getKeyCountForBucketIndex(int bucketIndex) {
        var offset = bucketIndex * ONE_LENGTH;
        return inMemoryCachedByteBuffer.getShort(offset);
    }

    long getKeyCount() {
        long keyCount = 0;
        for (int i = 0; i < bucketsPerSlot; i++) {
            var offset = i * ONE_LENGTH;
            keyCount += inMemoryCachedByteBuffer.getShort(offset);
        }
        return keyCount;
    }

    void clear() {
        if (ConfForSlot.global.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            raf.seek(0);
            raf.write(tmpBytes);
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void cleanUp() {
        if (ConfForSlot.global.pureMemory) {
            return;
        }

        // sync all
        try {
            raf.seek(0);
            raf.write(inMemoryCachedBytes);
            System.out.println("Stat key count in buckets sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
