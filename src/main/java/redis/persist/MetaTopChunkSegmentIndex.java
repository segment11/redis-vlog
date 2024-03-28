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

public class MetaTopChunkSegmentIndex {
    private static final String META_TOP_CHUNK_SEGMENT_INDEX_FILE = "meta_top_chunk_segment_index.dat";
    public static final int ONE_LENGTH = 4;

    private final short slotNumber;
    private final byte topMergeWorkers;
    private final int topMergeWorkerBeginIndex;
    private final int oneBatchCapacity;
    private final int oneWorkerCapacity;
    private final int allCapacity;
    private final RandomAccessFile raf;

    // 16KB
    private static final int BATCH_SIZE = 16 * 1024;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    public byte[] getInMemoryCachedBytes() {
        return inMemoryCachedBytes;
    }

    public synchronized void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl meta top chunk segment index, bytes length not match");
        }

        try {
            raf.seek(0);
            raf.write(bytes);
            System.arraycopy(bytes, 0, inMemoryCachedBytes, 0, bytes.length);
        } catch (IOException e) {
            throw new RuntimeException("Repl meta key bucket split number, write file error", e);
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaTopChunkSegmentIndex(short slotNumber, byte topMergeWorkers, int topMergeWorkerBeginIndex, File persistDir) throws IOException {
        this.slotNumber = slotNumber;
        this.topMergeWorkers = topMergeWorkers;
        this.topMergeWorkerBeginIndex = topMergeWorkerBeginIndex;

        this.oneBatchCapacity = slotNumber * ONE_LENGTH;
        this.oneWorkerCapacity = ConfForSlot.global.confWal.batchNumber * oneBatchCapacity;
        this.allCapacity = topMergeWorkers * oneWorkerCapacity;

        // top merge workers <= 32, batch number <= 2, slot number <= 128, 32 * 2 * 128 * 4 = 32KB
        this.inMemoryCachedBytes = new byte[allCapacity];

        boolean needRead = false;
        var file = new File(persistDir, META_TOP_CHUNK_SEGMENT_INDEX_FILE);
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
            log.warn("Read meta top chunk segment index file success, file: {}, all capacity: {}KB",
                    file, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    public synchronized void put(byte workerId, byte batchIndex, byte slot, int segmentIndex) {
        var bytes = new byte[ONE_LENGTH];
        ByteBuffer.wrap(bytes).putInt(segmentIndex);

        var offset = (workerId - topMergeWorkerBeginIndex) * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                slot * ONE_LENGTH;
        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.putInt(offset, segmentIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized int get(byte workerId, byte batchIndex, byte slot) {
        var offset = (workerId - topMergeWorkerBeginIndex) * oneWorkerCapacity +
                batchIndex * oneBatchCapacity +
                slot * ONE_LENGTH;
        return inMemoryCachedByteBuffer.getInt(offset);
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
