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

public class MetaChunkSegmentIndex {
    private static final String META_CHUNK_SEGMENT_INDEX_FILE = "meta_chunk_segment_index.dat";
    public static final int ONE_LENGTH = 4;

    private final byte slot;
    private final byte allWorkers;
    private final int oneWorkerCapacity;
    private final int allCapacity;
    private final RandomAccessFile raf;

    // 1KB
    private static final int BATCH_SIZE = 1024;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    public byte[] getInMemoryCachedBytes() {
        return inMemoryCachedBytes;
    }

    public synchronized void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl meta chunk segment index, bytes length not match");
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

    public MetaChunkSegmentIndex(byte slot, byte allWorkers, File slotDir) throws IOException {
        this.slot = slot;
        this.allWorkers = allWorkers;

        this.oneWorkerCapacity = ONE_LENGTH * ConfForSlot.global.confWal.batchNumber;
        this.allCapacity = allWorkers * oneWorkerCapacity;

        // max all workers <= 128, batch number <= 2, 128 * 2 * 4 = 1024
        this.inMemoryCachedBytes = new byte[allCapacity];

        boolean needRead = false;
        var file = new File(slotDir, META_CHUNK_SEGMENT_INDEX_FILE);
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
            log.warn("Read meta chunk segment index file success, file: {}, slot: {}, all capacity: {}B",
                    file, slot, allCapacity);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    public synchronized void put(byte workerId, byte batchIndex, int segmentIndex) {
        var bytes = new byte[ONE_LENGTH];
        ByteBuffer.wrap(bytes).putInt(segmentIndex);

        var offset = workerId * oneWorkerCapacity + batchIndex * ONE_LENGTH;
        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.putInt(offset, segmentIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized int get(byte workerId, byte batchIndex) {
        var offset = workerId * oneWorkerCapacity + batchIndex * ONE_LENGTH;
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
