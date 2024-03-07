package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TopChunkSegmentIndexMmapBuffer {
    private final File file;
    private final byte topMergeWorkers;
    private final int topMergeWorkerBeginIndex;
    private final short slotNumber;

    // segment index int
    private static final int SEGMENT_INDEX_BYTES_LENGTH = 4;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public TopChunkSegmentIndexMmapBuffer(File file, byte topMergeWorkers, int topMergeWorkerBeginIndex, short slotNumber) {
        this.file = file;
        this.topMergeWorkers = topMergeWorkers;
        this.topMergeWorkerBeginIndex = topMergeWorkerBeginIndex;
        this.slotNumber = slotNumber;
    }

    private LibC libC;

    public void setLibC(LibC libC) {
        this.libC = libC;
    }

    private int oneWorkerCapacity;
    private int capacity;

    private MmapBuffer mmapBuffer;

    public void init() throws IOException {
        this.oneWorkerCapacity = SEGMENT_INDEX_BYTES_LENGTH * slotNumber;
        this.capacity = topMergeWorkers * oneWorkerCapacity;
        // padding to 4KB
        if (capacity % 4096 != 0) {
            capacity = (capacity / 4096 + 1) * 4096;
        }

        mmapBuffer = new MmapBuffer(file, capacity);
        mmapBuffer.setLibC(libC);
        mmapBuffer.init((byte) 0);

        log.info("Top chunk segment index mmap buffer init success, file: {}, capacity: {}KB",
                file, capacity / 1024);
    }

    public void setTopMergeSegmentIndex(byte workerId, byte slot, int segmentIndex) {
        var bytes = new byte[SEGMENT_INDEX_BYTES_LENGTH];
        ByteBuffer.wrap(bytes).putInt(segmentIndex);

        var offset = (workerId - topMergeWorkerBeginIndex) * oneWorkerCapacity + slot * SEGMENT_INDEX_BYTES_LENGTH;
        mmapBuffer.write(offset, bytes, true);
    }

    public int getTopMergeSegmentIndex(byte workerId, byte slot) {
        var offset = (workerId - topMergeWorkerBeginIndex) * oneWorkerCapacity + slot * SEGMENT_INDEX_BYTES_LENGTH;
        return mmapBuffer.getInt(offset);
    }

    public void flush() {
        mmapBuffer.write(0, new byte[capacity], true);
        log.warn("Flush top chunk segment index mmap buffer");
    }

    public void cleanUp() {
        mmapBuffer.cleanUp();
    }
}
