package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static redis.persist.KeyLoader.KEY_BUCKET_COUNT_PER_FD;

// for stats
public class KeyBucketLastUpdateSizeMmapBuffer {
    private final File file;
    private final byte slot;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public KeyBucketLastUpdateSizeMmapBuffer(File file, byte slot) {
        this.file = file;
        this.slot = slot;
    }

    private LibC libC;

    public void setLibC(LibC libC) {
        this.libC = libC;
    }

    private int capacity;

    private MmapBuffer mmapBuffer;

    public void init() throws IOException {
        // size use short 2 bytes
        this.capacity = KeyBucket.MAX_BUCKETS_PER_SLOT * 2;
        log.info("One slot capacity: {}MB", capacity / 1024 / 1024);

        mmapBuffer = new MmapBuffer(file, capacity);
        mmapBuffer.setLibC(libC);
        mmapBuffer.init((byte) 0);

        log.info("Key bucket last update size mmap buffer init success, file: {}, slot: {}, capacity: {}MB",
                file, slot, capacity / 1024 / 1024);
    }

    public interface IterateCallBack {
        void call(byte slot, int bucketIndex, short size);
    }

    public void iterate(IterateCallBack callBack) {
        for (int i = 0; i < capacity; i += 2) {
            var bucketIndex = i / 2;

            short size = mmapBuffer.getShort(i);
            callBack.call(slot, bucketIndex, size);
        }
    }

    public void setBucketIndexKeyCount(int bucketIndex, short keyCount, boolean isSync) {
        var offset = bucketIndex * 2;
        var bytes = new byte[2];
        ByteBuffer.wrap(bytes).putShort(keyCount);
        mmapBuffer.write(offset, bytes, isSync);
    }

    public short getKeyCountInBucketIndex(int bucketIndex) {
        var offset = bucketIndex * 2;
        return mmapBuffer.getShort(offset);
    }

    public long getKeyCount() {
        long keyCount = 0;
        for (int i = 0; i < KEY_BUCKET_COUNT_PER_FD; i++) {
            var offset = i * 2;
            keyCount += mmapBuffer.getShort(offset);
        }
        return keyCount;
    }

    public void cleanUp() {
        mmapBuffer.cleanUp();
    }
}
