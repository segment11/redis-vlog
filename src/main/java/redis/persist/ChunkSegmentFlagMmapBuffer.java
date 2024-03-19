package redis.persist;

import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ChunkSegmentFlagMmapBuffer {
    private final File file;
    private final byte allWorkers;
    // flag byte + merge worker id byte + segment seq long
    static final int SEGMENT_FLAG_BYTES_LENGTH = 1 + 1 + 8;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public ChunkSegmentFlagMmapBuffer(File file, byte allWorkers) {
        this.file = file;
        this.allWorkers = allWorkers;
    }

    private LibC libC;

    public void setLibC(LibC libC) {
        this.libC = libC;
    }

    private int oneBatchCapacity;
    private int oneWorkerCapacity;
    private int capacity;

    private MmapBuffer mmapBuffer;

    public void init() throws IOException {
        this.oneBatchCapacity = ConfForSlot.global.confChunk.maxSegmentNumber() * SEGMENT_FLAG_BYTES_LENGTH;
        log.info("One batch capacity: {}MB", oneBatchCapacity / 1024 / 1024);
        this.oneWorkerCapacity = oneBatchCapacity * ConfForSlot.global.confWal.batchNumber;
        log.info("One worker capacity: {}MB", oneWorkerCapacity / 1024 / 1024);
        this.capacity = allWorkers * oneWorkerCapacity;
        // overflow
        if (this.capacity < 0) {
            throw new IllegalArgumentException("Capacity is too large, overflow");
        }

        mmapBuffer = new MmapBuffer(file, capacity);
        mmapBuffer.setLibC(libC);
        mmapBuffer.init(Chunk.SEGMENT_FLAG_INIT);

        log.info("Chunk segment flag mmap buffer init success, file: {}, capacity: {}MB",
                file, capacity / 1024 / 1024);
    }

    public interface IterateCallBack {
        void call(byte workerId, byte batchIndex, int segmentIndex, byte flag, byte mergeWorkerId, long segmentSeq);
    }

    public void iterate(IterateCallBack callBack) {
        for (int i = 0; i < capacity; i += SEGMENT_FLAG_BYTES_LENGTH) {
            var workerId = (byte) (i / oneWorkerCapacity);
            var batchIndex = (i % oneWorkerCapacity) / oneBatchCapacity;
            var segmentIndex = (i % oneBatchCapacity) / SEGMENT_FLAG_BYTES_LENGTH;

            var flag = mmapBuffer.getByte(i);
            var mergeWorkerId = mmapBuffer.getByte(i + 1);
            var segmentSeq = mmapBuffer.getLong(i + 2);

            callBack.call(workerId, (byte) batchIndex, segmentIndex, flag, mergeWorkerId, segmentSeq);
        }
    }

    public void setSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex, byte flag, byte mergeWorkerId, long segmentSeq) {
        var offset = workerId * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                segmentIndex * SEGMENT_FLAG_BYTES_LENGTH;
        var bytes = new byte[SEGMENT_FLAG_BYTES_LENGTH];
        bytes[0] = flag;
        bytes[1] = mergeWorkerId;
        ByteBuffer.wrap(bytes, 2, 8).putLong(segmentSeq);
        mmapBuffer.write(offset, bytes, true);
    }

    public void setSegmentMergeFlagBatch(byte workerId, byte batchIndex, int segmentIndex, byte[] bytes) {
        var offset = workerId * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                segmentIndex * SEGMENT_FLAG_BYTES_LENGTH;
        mmapBuffer.write(offset, bytes, true);
    }

    public byte[] getSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex) {
        var offset = workerId * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                segmentIndex * SEGMENT_FLAG_BYTES_LENGTH;
        return mmapBuffer.getBytes(offset, SEGMENT_FLAG_BYTES_LENGTH);
    }

    public void flush() {
        var data = new byte[capacity];
        Arrays.fill(data, Chunk.SEGMENT_FLAG_INIT);
        mmapBuffer.write(0, data, true);
        log.warn("Flush chunk segment flag mmap buffer");
    }

    public void cleanUp() {
        mmapBuffer.cleanUp();
    }
}
