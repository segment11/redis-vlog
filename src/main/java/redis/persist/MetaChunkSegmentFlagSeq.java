package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MetaChunkSegmentFlagSeq {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    // flag byte + seq long
    public static final int ONE_LENGTH = 1 + 8;

    private final byte slot;
    private final int allCapacity;
    private RandomAccessFile raf;

    private static void fillSegmentFlagInit(byte[] innerBytes) {
        var innerBuffer = ByteBuffer.wrap(innerBytes);
        var times = innerBytes.length / ONE_LENGTH;
        for (int i = 0; i < times; i++) {
            innerBuffer.put(Chunk.SEGMENT_FLAG_INIT);
            innerBuffer.position(innerBuffer.position() + 8);
        }
    }

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    byte[] getInMemoryCachedBytes() {
        var dst = new byte[inMemoryCachedBytes.length];
        inMemoryCachedByteBuffer.position(0).get(dst);
        return dst;
    }

    void overwriteInMemoryCachedBytes(byte[] bytes) {
        if (bytes.length != inMemoryCachedBytes.length) {
            throw new IllegalArgumentException("Repl chunk segment flag seq, bytes length not match");
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
            throw new RuntimeException("Repl chunk segment flag seq, write file error", e);
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaChunkSegmentFlagSeq(byte slot, File slotDir) throws IOException {
        this.slot = slot;
        this.allCapacity = ConfForSlot.global.confChunk.maxSegmentNumber() * ONE_LENGTH;

        // max max segment number <= 512KB * 8, 512KB * 8 * 9 = 36MB
        this.inMemoryCachedBytes = new byte[allCapacity];
        fillSegmentFlagInit(inMemoryCachedBytes);

        if (ConfForSlot.global.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_CHUNK_SEGMENT_SEQ_FLAG_FILE);
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
            log.warn("Read meta chunk segment flag seq file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    interface IterateCallBack {
        void call(int segmentIndex, byte flag, long segmentSeq);
    }

    void iterate(IterateCallBack callBack) {
        for (int i = 0; i < allCapacity; i += ONE_LENGTH) {
            var segmentIndex = i;

            var flag = inMemoryCachedByteBuffer.get(i);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(i + 1);

            callBack.call(segmentIndex, flag, segmentSeq);
        }
    }

    List<Long> getSegmentSeqListBatchForRepl(int beginSegmentIndex, int segmentCount) {
        var offset = beginSegmentIndex * ONE_LENGTH;
        var list = new ArrayList<Long>();
        for (int i = beginSegmentIndex; i < beginSegmentIndex + segmentCount; i++) {
            list.add(inMemoryCachedByteBuffer.getLong(offset + 1));
            offset += ONE_LENGTH;
        }
        return list;
    }

    void setSegmentMergeFlag(int segmentIndex, byte flag, long segmentSeq) {
        var offset = segmentIndex * ONE_LENGTH;
        var bytes = new byte[ONE_LENGTH];
        bytes[0] = flag;
        ByteBuffer.wrap(bytes, 1, 8).putLong(segmentSeq);

        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.put(offset, bytes);
            return;
        }

        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.put(offset, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void setSegmentMergeFlagBatch(int segmentIndex, int segmentCount, byte flag, List<Long> segmentSeqList) {
        var bytes = new byte[segmentCount * ONE_LENGTH];
        var buffer = ByteBuffer.wrap(bytes);
        for (int i = 0; i < segmentCount; i++) {
            buffer.put(i * ONE_LENGTH, flag);
            buffer.putLong(i * ONE_LENGTH + 1, segmentSeqList.get(i));
        }

        var offset = segmentIndex * ONE_LENGTH;
        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.put(offset, bytes);
            return;
        }

        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.put(offset, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Chunk.SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        var offset = segmentIndex * ONE_LENGTH;
        return new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset), inMemoryCachedByteBuffer.getLong(offset + 1));
    }

    void clear() {
        if (ConfForSlot.global.pureMemory) {
            fillSegmentFlagInit(inMemoryCachedBytes);
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            fillSegmentFlagInit(tmpBytes);
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
//            System.out.println("Meta chunk segment flag seq sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
