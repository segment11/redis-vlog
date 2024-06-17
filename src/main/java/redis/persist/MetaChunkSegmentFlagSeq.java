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
import java.util.Arrays;
import java.util.List;

import static redis.persist.Chunk.*;
import static redis.persist.FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT;

public class MetaChunkSegmentFlagSeq {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    // flag byte + seq long + wal group index int
    public static final int ONE_LENGTH = 1 + 8 + 4;

    private final byte slot;
    private final int maxSegmentNumber;
    private final int allCapacity;
    private RandomAccessFile raf;

    private static void fillSegmentFlagInit(byte[] innerBytes) {
        var innerBuffer = ByteBuffer.wrap(innerBytes);
        var times = innerBytes.length / ONE_LENGTH;
        for (int i = 0; i < times; i++) {
            innerBuffer.put(Chunk.SEGMENT_FLAG_INIT);
            innerBuffer.position(innerBuffer.position() + 8 + 4);
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
        this.maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        this.allCapacity = maxSegmentNumber * ONE_LENGTH;

        // max max segment number <= 512KB * 8, 512KB * 8 * 13 = 56MB
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
        void call(int segmentIndex, byte flag, long segmentSeq, int walGroupIndex);
    }

    int[] iterateAndFind(int beginSegmentIndex, int nextSegmentCount, int targetWalGroupIndex, Chunk chunk) {
        var findSegmentIndexWithSegmentCount = new int[]{NO_NEED_MERGE_SEGMENT_INDEX, 0};

        var end = Math.min(beginSegmentIndex + nextSegmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            if (findSegmentIndexWithSegmentCount[0] != NO_NEED_MERGE_SEGMENT_INDEX) {
                break;
            }

            var offset = segmentIndex * ONE_LENGTH;

            var flag = inMemoryCachedByteBuffer.get(offset);
//            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            if (walGroupIndex != targetWalGroupIndex) {
                continue;
            }

            if (flag == SEGMENT_FLAG_NEW || flag == SEGMENT_FLAG_REUSE_AND_PERSISTED) {
                findSegmentIndexWithSegmentCount[0] = segmentIndex;

                int segmentCount = Math.min(MERGE_READ_ONCE_SEGMENT_COUNT, chunk.maxSegmentIndex - segmentIndex + 1);
                var targetFdIndex = chunk.targetFdIndex(segmentIndex);
                var targetFdIndexEnd = chunk.targetFdIndex(segmentIndex + segmentCount - 1);
                // cross two files, just read one segment
                if (targetFdIndexEnd != targetFdIndex) {
                    segmentCount = 1;
                }

                findSegmentIndexWithSegmentCount[1] = segmentCount;
            }
        }

        return findSegmentIndexWithSegmentCount;
    }

    void iterateRange(int beginSegmentIndex, int segmentCount, IterateCallBack callBack) {
        var end = Math.min(beginSegmentIndex + segmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flag = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, flag, segmentSeq, walGroupIndex);
        }
    }

    void iterateAll(IterateCallBack callBack) {
        for (int segmentIndex = 0; segmentIndex < ConfForSlot.global.confChunk.maxSegmentNumber(); segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flag = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, flag, segmentSeq, walGroupIndex);
        }
    }

    int getMergedSegmentIndexEndLastTime(int currentSegmentIndex, int halfSegmentNumber) {
        var tmpBytes = new byte[allCapacity];
        fillSegmentFlagInit(tmpBytes);
        var isAllFlagInit = Arrays.equals(inMemoryCachedBytes, tmpBytes);
        if (isAllFlagInit) {
            return NO_NEED_MERGE_SEGMENT_INDEX;
        }

        var max = NO_NEED_MERGE_SEGMENT_INDEX;

        int begin = 0;
        int end = halfSegmentNumber;
        if (currentSegmentIndex < halfSegmentNumber) {
            begin = halfSegmentNumber;
            end = ConfForSlot.global.confChunk.maxSegmentNumber();
        }

        for (int segmentIndex = begin; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flag = inMemoryCachedByteBuffer.get(offset);
            if (flag == SEGMENT_FLAG_MERGED || flag == SEGMENT_FLAG_MERGED_AND_PERSISTED || flag == SEGMENT_FLAG_INIT) {
                max = segmentIndex;
            } else {
                break;
            }
        }

        return max;
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

    void setSegmentMergeFlag(int segmentIndex, byte flag, long segmentSeq, int walGroupIndex) {
        var offset = segmentIndex * ONE_LENGTH;
        var bytes = new byte[ONE_LENGTH];
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        wrap.put(flag);
        wrap.putLong(segmentSeq);
        wrap.putInt(walGroupIndex);

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

    void setSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount, byte flag, List<Long> segmentSeqList, int walGroupIndex) {
        var bytes = new byte[segmentCount * ONE_LENGTH];
        var wrap = ByteBuffer.wrap(bytes);
        for (int i = 0; i < segmentCount; i++) {
            wrap.put(i * ONE_LENGTH, flag);
            wrap.putLong(i * ONE_LENGTH + 1, segmentSeqList.get(i));
            wrap.putInt(i * ONE_LENGTH + 1 + 8, walGroupIndex);
        }

        var offset = beginSegmentIndex * ONE_LENGTH;
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
        return new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset),
                inMemoryCachedByteBuffer.getLong(offset + 1),
                inMemoryCachedByteBuffer.getInt(offset + 1 + 8));
    }

    ArrayList<Chunk.SegmentFlag> getSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount) {
        var list = new ArrayList<Chunk.SegmentFlag>(segmentCount);
        var offset = beginSegmentIndex * ONE_LENGTH;
        for (int i = 0; i < segmentCount; i++) {
            list.add(new Chunk.SegmentFlag(inMemoryCachedByteBuffer.get(offset),
                    inMemoryCachedByteBuffer.getLong(offset + 1),
                    inMemoryCachedByteBuffer.getInt(offset + 1 + 8)));
            offset += ONE_LENGTH;
        }
        return list;
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
