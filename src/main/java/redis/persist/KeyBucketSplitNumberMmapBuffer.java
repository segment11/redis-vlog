package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

// for stats
public class KeyBucketSplitNumberMmapBuffer {
    private final File file;
    private final int bucketsPerSlot;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public KeyBucketSplitNumberMmapBuffer(File file, int bucketsPerSlot) {
        this.file = file;
        this.bucketsPerSlot = bucketsPerSlot;
    }

    private LibC libC;

    public void setLibC(LibC libC) {
        this.libC = libC;
    }

    private int capacity;

    private MmapBuffer mmapBuffer;

    public void init() throws IOException {
        this.capacity = bucketsPerSlot;

        mmapBuffer = new MmapBuffer(file, capacity);
        mmapBuffer.setLibC(libC);
        // default split number is 1
        mmapBuffer.init((byte) 1);

        log.info("Key bucket split number mmap buffer init success, file: {}, buckets per slot: {}, capacity: {}MB",
                file, bucketsPerSlot, capacity / 1024 / 1024);
    }

    public void set(int bucketIndex, byte splitNumber) {
        var offset = bucketIndex;
        var bytes = new byte[]{splitNumber};
        mmapBuffer.write(offset, bytes, true);
    }

    public byte get(int bucketIndex) {
        var offset = bucketIndex;
        return mmapBuffer.getByte(offset);
    }

    void clear() {
        var bytes = new byte[capacity];
        Arrays.fill(bytes, (byte) 1);
        mmapBuffer.write(0, bytes, true);
    }

    public byte maxSplitNumber() {
        byte max = 1;
        for (int i = 0; i < capacity; i++) {
            byte splitNumber = mmapBuffer.getByte(i);
            if (splitNumber > max) {
                max = splitNumber;
            }
        }
        return max;
    }

    public void cleanUp() {
        mmapBuffer.cleanUp();
    }
}
