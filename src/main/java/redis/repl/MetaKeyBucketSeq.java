package redis.repl;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static redis.persist.KeyLoader.MAX_SPLIT_NUMBER;

// for repl increase copy, need not copy all data, just copy key buckets those seq not matched
public class MetaKeyBucketSeq {
    private static final String META_KEY_BUCKET_SEQ_FILE = "meta_key_bucket_seq.dat";

    private final byte slot;
    private final int bucketsPerSlot;
    private final int oneSplitCapacity;
    private final int allCapacity;
    private final RandomAccessFile raf;

    private static final int BATCH_SIZE = Long.BYTES * MAX_SPLIT_NUMBER * 1024;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaKeyBucketSeq(byte slot, int bucketsPerSlot, File slotDir) throws IOException {
        this.slot = slot;
        this.bucketsPerSlot = bucketsPerSlot;
        this.oneSplitCapacity = bucketsPerSlot * Long.BYTES;
        this.allCapacity = MAX_SPLIT_NUMBER * oneSplitCapacity;

        this.inMemoryCachedBytes = new byte[allCapacity];

        boolean needRead = false;
        var file = new File(slotDir, META_KEY_BUCKET_SEQ_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);

            // bucketsPerSlot % 1024 == 0
            var initTimes = allCapacity / BATCH_SIZE;
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
            log.warn("Read meta key bucket seq file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    public synchronized void writeSeq(int bucketIndex, byte splitIndex, long seq) {
        var offset = splitIndex * oneSplitCapacity + bucketIndex * Long.BYTES;
        try {
            raf.seek(offset);
            raf.writeLong(seq);
            inMemoryCachedByteBuffer.putLong(offset, seq);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void writeSeqBatch(int beginBucketIndex, byte splitIndex, ArrayList<Long> seqList) {
        var bytes = new byte[seqList.size() * Long.BYTES];
        var buffer = ByteBuffer.wrap(bytes);
        for (var seq : seqList) {
            buffer.putLong(seq);
        }

        var offset = splitIndex * oneSplitCapacity + beginBucketIndex * Long.BYTES;
        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.position(offset).put(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized long readSeq(int bucketIndex, byte splitIndex) {
        var offset = splitIndex * oneSplitCapacity + bucketIndex * Long.BYTES;
        return inMemoryCachedByteBuffer.getLong(offset);
    }

    record OneSeq(long seq, int bucketIndex, int splitIndex) {
    }

    public synchronized ArrayList<OneSeq> readSeqBatch(int beginBucketIndex, byte splitIndex, int size) {
        var offset = splitIndex * oneSplitCapacity + beginBucketIndex * Long.BYTES;
        inMemoryCachedByteBuffer.position(offset);

        var seqList = new ArrayList<OneSeq>();
        for (int i = 0; i < size; i++) {
            seqList.add(new OneSeq(inMemoryCachedByteBuffer.getLong(), beginBucketIndex + i, splitIndex));
        }
        return seqList;
    }

    public synchronized void clear() {
        var initTimes = bucketsPerSlot / 1024;
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
