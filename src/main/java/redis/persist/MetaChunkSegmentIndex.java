package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class MetaChunkSegmentIndex {
    private static final String META_CHUNK_SEGMENT_INDEX_FILE = "meta_chunk_segment_index.dat";
    private final byte slot;
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
            throw new IllegalArgumentException("Repl meta chunk segment index, bytes length not match");
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
            throw new RuntimeException("Repl meta key bucket split number, write file error", e);
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaChunkSegmentIndex(byte slot, File slotDir) throws IOException {
        this.slot = slot;
        this.inMemoryCachedBytes = new byte[Integer.BYTES];

        if (ConfForSlot.global.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_CHUNK_SEGMENT_INDEX_FILE);
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
            log.warn("Read meta chunk segment index file success, file: {}, slot: {}, segment index: {}",
                    file, slot, ByteBuffer.wrap(inMemoryCachedBytes).getInt(0));
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    void set(int segmentIndex) {
        if (ConfForSlot.global.pureMemory) {
            this.inMemoryCachedByteBuffer.putInt(0, segmentIndex);
            return;
        }

        try {
            raf.seek(0);
            raf.writeInt(segmentIndex);
            inMemoryCachedByteBuffer.putInt(0, segmentIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    int get() {
        return inMemoryCachedByteBuffer.getInt(0);
    }

    void clear() {
        set(0);
    }

    void cleanUp() {
        if (ConfForSlot.global.pureMemory) {
            return;
        }

        // sync all
        try {
//            raf.getFD().sync();
//            System.out.println("Meta chunk segment index sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
