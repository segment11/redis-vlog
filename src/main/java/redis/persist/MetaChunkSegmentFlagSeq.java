package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForGlobal;
import redis.ConfForSlot;
import redis.StaticMemoryPrepareBytesStats;
import redis.repl.SlaveNeedReplay;
import redis.repl.SlaveReplay;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static redis.persist.Chunk.Flag;
import static redis.persist.Chunk.NO_NEED_MERGE_SEGMENT_INDEX;

public class MetaChunkSegmentFlagSeq implements InMemoryEstimate {
    private static final String META_CHUNK_SEGMENT_SEQ_FLAG_FILE = "meta_chunk_segment_flag_seq.dat";
    // flag byte + seq long + wal group index int
    public static final int ONE_LENGTH = 1 + 8 + 4;

    private static final int INIT_WAL_GROUP_INDEX = -1;

    private final byte slot;
    private final int maxSegmentNumber;
    final int allCapacity;
    private RandomAccessFile raf;

    private static void fillSegmentFlagInit(byte[] innerBytes) {
        var initBytes = new byte[ONE_LENGTH];
        var initBuffer = ByteBuffer.wrap(initBytes);
        initBuffer.put(Flag.init.flagByte);
        initBuffer.putLong(0L);
        initBuffer.putInt(INIT_WAL_GROUP_INDEX);

        var times = innerBytes.length / ONE_LENGTH;
        var innerBuffer = ByteBuffer.wrap(innerBytes);
        for (int i = 0; i < times; i++) {
            innerBuffer.put(initBytes);
        }
    }

    final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    public byte[] getOneBatch(int beginBucketIndex, int bucketCount) {
        var dst = new byte[bucketCount * ONE_LENGTH];
        var offset = beginBucketIndex * ONE_LENGTH;
        inMemoryCachedByteBuffer.position(offset).get(dst);
        return dst;
    }

    public void overwriteOneBatch(byte[] bytes, int beginBucketIndex, int bucketCount) {
        if (bytes.length != bucketCount * ONE_LENGTH) {
            throw new IllegalArgumentException("Repl chunk segments from master one batch meta bytes length not match, slot: "
                    + slot + ", length: " + bytes.length + ", bucket count: " + bucketCount + ", one length: " + ONE_LENGTH);
        }

        var offset = beginBucketIndex * ONE_LENGTH;
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.position(offset).put(bytes);
            return;
        }

        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.position(offset).put(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Repl chunk segments from master one batch meta bytes, write file error", e);
        }
        log.warn("Repl chunk segments from master one batch meta bytes, write file success, slot: {}, begin bucket index: {}, bucket count: {}",
                slot, beginBucketIndex, bucketCount);
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaChunkSegmentFlagSeq(byte slot, File slotDir) throws IOException {
        this.slot = slot;
        this.maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        this.allCapacity = maxSegmentNumber * ONE_LENGTH;

        // max max segment number <= 512KB * 8, 512KB * 8 * 13 = 56MB
        this.inMemoryCachedBytes = new byte[allCapacity];
        fillSegmentFlagInit(inMemoryCachedBytes);

        if (ConfForGlobal.pureMemory) {
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

        var initMemoryMB = allCapacity / 1024 / 1024;
        log.info("Static memory init, type: {}, MB: {}, slot: {}", StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq, initMemoryMB, true);
    }

    @Override
    public long estimate() {
        return allCapacity;
    }

    public interface IterateCallBack {
        void call(int segmentIndex, Flag flag, long segmentSeq, int walGroupIndex);
    }

    int[] iterateAndFindThoseNeedToMerge(int beginSegmentIndex, int nextSegmentCount, int targetWalGroupIndex, Chunk chunk) {
        var findSegmentIndexWithSegmentCount = new int[]{NO_NEED_MERGE_SEGMENT_INDEX, 0};

        // only find 2 segments at most, or once write too many segments for this batch
        final var maxFindSegmentCount = 2;
        var segmentCount = 0;
        var end = Math.min(beginSegmentIndex + nextSegmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var flag = Flag.fromFlagByte(flagByte);
//            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);
            if (walGroupIndex != targetWalGroupIndex) {
                // already find at least one segment with the same wal group index
                if (findSegmentIndexWithSegmentCount[0] != NO_NEED_MERGE_SEGMENT_INDEX) {
                    break;
                }
                continue;
            }

            if (flag == Flag.new_write || flag == Flag.reuse_new) {
                // only set first segment index
                if (findSegmentIndexWithSegmentCount[0] == NO_NEED_MERGE_SEGMENT_INDEX) {
                    findSegmentIndexWithSegmentCount[0] = segmentIndex;
                }
                segmentCount++;

                var targetFdIndexFirstFind = chunk.targetFdIndex(findSegmentIndexWithSegmentCount[0]);
                var targetFdIndexThisFind = chunk.targetFdIndex(segmentIndex);
                // cross two files, exclude this find segment
                if (targetFdIndexThisFind != targetFdIndexFirstFind) {
                    segmentCount--;
                    break;
                }

                int segmentCountMax = Math.min(maxFindSegmentCount, chunk.maxSegmentIndex - segmentIndex + 1);
                if (segmentCount >= segmentCountMax) {
                    break;
                }
            }
        }

        findSegmentIndexWithSegmentCount[1] = segmentCount;
        return findSegmentIndexWithSegmentCount;
    }

    public void iterateRange(int beginSegmentIndex, int segmentCount, IterateCallBack callBack) {
        var end = Math.min(beginSegmentIndex + segmentCount, maxSegmentNumber);
        for (int segmentIndex = beginSegmentIndex; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, Flag.fromFlagByte(flagByte), segmentSeq, walGroupIndex);
        }
    }

    public void iterateAll(IterateCallBack callBack) {
        for (int segmentIndex = 0; segmentIndex < ConfForSlot.global.confChunk.maxSegmentNumber(); segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var segmentSeq = inMemoryCachedByteBuffer.getLong(offset + 1);
            var walGroupIndex = inMemoryCachedByteBuffer.getInt(offset + 1 + 8);

            callBack.call(segmentIndex, Flag.fromFlagByte(flagByte), segmentSeq, walGroupIndex);
        }
    }

    int getMergedSegmentIndexEndLastTime(int currentSegmentIndex, int halfSegmentNumber) {
        // only execute once when server start, do not mind performance
        boolean isAllFlagInit = true;
        for (int i = 0; i < allCapacity; i += ONE_LENGTH) {
            if (inMemoryCachedBytes[i] != Flag.init.flagByte) {
                isAllFlagInit = false;
                break;
            }
        }
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
        log.info("Get merged segment index end last time, current segment index: {}, half segment number: {}, begin: {}, end: {}, slot: {}",
                currentSegmentIndex, halfSegmentNumber, begin, end, slot);

        boolean isAllFlagInitHalf = true;
        for (int segmentIndex = begin; segmentIndex < end; segmentIndex++) {
            var offset = segmentIndex * ONE_LENGTH;

            var flagByte = inMemoryCachedByteBuffer.get(offset);
            var flag = Flag.fromFlagByte(flagByte);
            if (flag != Flag.init) {
                isAllFlagInitHalf = false;
            }
            if (flag == Flag.merged || flag == Flag.merged_and_persisted || flag == Flag.init) {
                max = segmentIndex;
            } else {
                break;
            }
        }

        if (isAllFlagInitHalf) {
            max = NO_NEED_MERGE_SEGMENT_INDEX;
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

    @SlaveNeedReplay
    public void setSegmentMergeFlag(int segmentIndex, Flag flag, long segmentSeq, int walGroupIndex) {
        var offset = segmentIndex * ONE_LENGTH;

        var bytes = new byte[ONE_LENGTH];
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        wrap.put(flag.flagByte);
        wrap.putLong(segmentSeq);
        wrap.putInt(walGroupIndex);

        StatKeyCountInBuckets.writeToRaf(offset, bytes, inMemoryCachedByteBuffer, raf);
    }

    public void setSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount, Flag flag, List<Long> segmentSeqList, int walGroupIndex) {
        var offset = beginSegmentIndex * ONE_LENGTH;

        var bytes = new byte[segmentCount * ONE_LENGTH];
        var wrap = ByteBuffer.wrap(bytes);
        for (int i = 0; i < segmentCount; i++) {
            wrap.put(i * ONE_LENGTH, flag.flagByte);
            wrap.putLong(i * ONE_LENGTH + 1, segmentSeqList.get(i));
            wrap.putInt(i * ONE_LENGTH + 1 + 8, walGroupIndex);
        }

        StatKeyCountInBuckets.writeToRaf(offset, bytes, inMemoryCachedByteBuffer, raf);
    }

    Chunk.SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        var offset = segmentIndex * ONE_LENGTH;
        return new Chunk.SegmentFlag(Flag.fromFlagByte(inMemoryCachedByteBuffer.get(offset)),
                inMemoryCachedByteBuffer.getLong(offset + 1),
                inMemoryCachedByteBuffer.getInt(offset + 1 + 8));
    }

    ArrayList<Chunk.SegmentFlag> getSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount) {
        var list = new ArrayList<Chunk.SegmentFlag>(segmentCount);
        var offset = beginSegmentIndex * ONE_LENGTH;
        for (int i = 0; i < segmentCount; i++) {
            list.add(new Chunk.SegmentFlag(Flag.fromFlagByte(inMemoryCachedByteBuffer.get(offset)),
                    inMemoryCachedByteBuffer.getLong(offset + 1),
                    inMemoryCachedByteBuffer.getInt(offset + 1 + 8)));
            offset += ONE_LENGTH;
        }
        return list;
    }

    @SlaveNeedReplay
    @SlaveReplay
    void clear() {
        if (ConfForGlobal.pureMemory) {
            fillSegmentFlagInit(inMemoryCachedBytes);
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            fillSegmentFlagInit(tmpBytes);
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
            System.out.println("Meta chunk segment flag seq clear done, set init flags.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void cleanUp() {
        if (ConfForGlobal.pureMemory) {
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
