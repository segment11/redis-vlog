package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class MetaChunkSegmentFlagSeq {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    // flag byte + merge worker byte + seq long
    public static final int ONE_LENGTH = 1 + 1 + 8;

    private final byte slot;
    private final byte allWorkers;
    private final int oneBatchCapacity;
    private final int oneWorkerCapacity;
    private final int allCapacity;
    private final RandomAccessFile raf;

    // 100KB
    private static final int BATCH_SIZE = 1024 * 100;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    private static void fillSegmentFlagInit(byte[] innerBytes) {
        var innerBuffer = ByteBuffer.wrap(innerBytes);
        var times = innerBytes.length / ONE_LENGTH;
        for (int i = 0; i < times; i++) {
            innerBuffer.put(Chunk.SEGMENT_FLAG_INIT);
            innerBuffer.position(innerBuffer.position() + 9);
        }
    }

    static {
        var innerBytes = new byte[100];
        fillSegmentFlagInit(innerBytes);

        var buffer = ByteBuffer.wrap(EMPTY_BYTES);
        for (int i = 0; i < 1024; i++) {
            buffer.put(innerBytes);
        }
    }

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    public byte[] getInMemoryCachedBytesOneWorker(byte workerId) {
        var bytes = new byte[oneWorkerCapacity + 1];
        bytes[0] = workerId;
        var offset = workerId * oneWorkerCapacity;
        System.arraycopy(inMemoryCachedBytes, offset, bytes, 1, oneWorkerCapacity);
        return bytes;
    }

    public synchronized void overwriteInMemoryCachedBytesOneWorker(byte[] bytes) {
        if (bytes.length != oneWorkerCapacity + 1) {
            throw new IllegalArgumentException("Repl meta chunk segment flag seq, bytes length not match");
        }

        var workerId = bytes[0];
        var offset = workerId * oneWorkerCapacity;
        try {
            raf.seek(offset);
            raf.write(bytes, 1, oneWorkerCapacity);
            System.arraycopy(bytes, 1, inMemoryCachedBytes, offset, oneWorkerCapacity);
        } catch (IOException e) {
            throw new RuntimeException("Repl meta chunk segment flag seq, write file error", e);
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaChunkSegmentFlagSeq(byte slot, byte allWorkers, File slotDir) throws IOException {
        this.slot = slot;
        this.allWorkers = allWorkers;

        this.oneBatchCapacity = ConfForSlot.global.confChunk.maxSegmentNumber() * ONE_LENGTH;
        this.oneWorkerCapacity = ConfForSlot.global.confWal.batchNumber * oneBatchCapacity;
        this.allCapacity = allWorkers * oneWorkerCapacity;

        // max all workers <= 128, batch number <= 2, max segment number <= 512KB, 128 * 2 * 512KB * 10 = 1.28GB
        this.inMemoryCachedBytes = new byte[allCapacity];
        fillSegmentFlagInit(inMemoryCachedBytes);

        boolean needRead = false;
        var file = new File(slotDir, META_CHUNK_SEGMENT_SEQ_FLAG_FILE);
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
            log.warn("Read meta chunk segment flag seq file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    public interface IterateCallBack {
        void call(byte workerId, byte batchIndex, int segmentIndex, byte flag, byte mergeWorkerId, long segmentSeq);
    }

    public synchronized void iterate(IterateCallBack callBack) {
        for (int i = 0; i < allCapacity; i += ONE_LENGTH) {
            var workerId = (byte) (i / oneWorkerCapacity);
            var batchIndex = (i % oneWorkerCapacity) / oneBatchCapacity;
            var segmentIndex = (i % oneBatchCapacity) / ONE_LENGTH;

            var flag = inMemoryCachedByteBuffer.get(i);
            var mergeWorkerId = inMemoryCachedByteBuffer.get(i + 1);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(i + 2);

            callBack.call(workerId, (byte) batchIndex, segmentIndex, flag, mergeWorkerId, segmentSeq);
        }
    }

    public synchronized void setSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex, byte flag, byte mergeWorkerId, long segmentSeq) {
        var offset = workerId * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                segmentIndex * ONE_LENGTH;
        var bytes = new byte[ONE_LENGTH];
        bytes[0] = flag;
        bytes[1] = mergeWorkerId;
        ByteBuffer.wrap(bytes, 2, 8).putLong(segmentSeq);
        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.put(offset, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void setSegmentMergeFlagBatch(byte workerId, byte batchIndex, int beginSegmentIndex, byte[] bytes) {
        var offset = workerId * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                beginSegmentIndex * ONE_LENGTH;
        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.put(offset, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Chunk.SegmentFlag getSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex) {
        var offset = workerId * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                segmentIndex * ONE_LENGTH;
        return new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset), inMemoryCachedByteBuffer.get(offset + 1),
                inMemoryCachedByteBuffer.getLong(offset + 2));
    }

    public synchronized void clear() {
        var initTimes = allCapacity / BATCH_SIZE;
        if (allCapacity % BATCH_SIZE != 0) {
            initTimes++;
        }
        try {
            for (int i = 0; i < initTimes; i++) {
                raf.seek(i * BATCH_SIZE);
                raf.write(EMPTY_BYTES);
            }
            fillSegmentFlagInit(inMemoryCachedBytes);
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
