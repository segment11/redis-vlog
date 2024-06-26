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
    final int allCapacity;
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
            throw new IllegalArgumentException("Repl stat key count in buckets, bytes length not match");
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
            throw new RuntimeException("Repl stat key count in buckets, write file error", e);
        }
    }

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
        log.info("Key count in buckets: {}, slot: {}", getKeyCount(), slot);
    }

    void setKeyCountForBucketIndex(int bucketIndex, short keyCount) {
        var offset = bucketIndex * ONE_LENGTH;

        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.putShort(offset, keyCount);
            return;
        }

        try {
            raf.seek(offset);
            raf.writeShort(keyCount);
            inMemoryCachedByteBuffer.putShort(offset, keyCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            Arrays.fill(tmpBytes, (byte) 0);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
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
//            raf.getFD().sync();
//            System.out.println("Stat key count in buckets sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
