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

public class StatKeyBucketLastUpdateCount {
    private static final String STAT_KEY_BUCKET_LAST_UPDATE_COUNT_FILE = "stat_key_bucket_last_update_count.dat";
    // short is enough for one key bucket total value count
    public static final int ONE_LENGTH = 2;

    private final byte slot;
    private final int bucketsPerSlot;
    private final int allCapacity;
    private RandomAccessFile raf;

    // 64KB
    private static final int BATCH_SIZE = 1024 * 64;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public StatKeyBucketLastUpdateCount(byte slot, int bucketsPerSlot, File slotDir) throws IOException {
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
            log.warn("Read stat key bucket last update count file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    public interface IterateCallBack {
        void call(byte slot, int bucketIndex, short size);
    }

    public void iterate(IterateCallBack callBack) {
        for (int bucketIndex = 0; bucketIndex < bucketsPerSlot; bucketIndex++) {
            var size = inMemoryCachedByteBuffer.getShort(bucketIndex);
            callBack.call(slot, bucketIndex, size);
        }
    }

    public void setKeyCountInBucketIndex(int bucketIndex, short keyCount, boolean isSync) {
        var offset = bucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.putShort(offset, keyCount);

        if (ConfForSlot.global.pureMemory) {
            return;
        }

        if (isSync) {
            var bytes = new byte[2];
            ByteBuffer.wrap(bytes).putShort(keyCount);
            try {
                raf.seek(offset);
                raf.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public short getKeyCountInBucketIndex(int bucketIndex) {
        var offset = bucketIndex * ONE_LENGTH;
        return inMemoryCachedByteBuffer.getShort(offset);
    }

    public long getKeyCount() {
        long keyCount = 0;
        for (int i = 0; i < bucketsPerSlot; i++) {
            var offset = i * ONE_LENGTH;
            keyCount += inMemoryCachedByteBuffer.getShort(offset);
        }
        return keyCount;
    }

    public synchronized void clear() {
        if (ConfForSlot.global.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            return;
        }

        var initTimes = allCapacity / BATCH_SIZE;
        if (allCapacity % BATCH_SIZE != 0) {
            initTimes++;
        }
        try {
            for (int i = 0; i < initTimes; i++) {
                raf.seek(i * BATCH_SIZE);
                raf.write(EMPTY_BYTES);
            }
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void cleanUp() {
        if (ConfForSlot.global.pureMemory) {
            return;
        }

        try {
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
