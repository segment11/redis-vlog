package redis.repl;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static redis.persist.KeyLoader.MAX_SPLIT_NUMBER;

public class MetaKeyBucketSeq {
    private static final String META_KEY_BUCKET_SEQ_FILE = "meta_key_bucket_seq.dat";

    private final byte slot;
    private final int bucketsPerSlot;
    private final RandomAccessFile raf;

    private static final int BATCH_SIZE = Long.BYTES * MAX_SPLIT_NUMBER * 1024;
    private static final byte[] EMPTY_BYTES = new byte[BATCH_SIZE];

    public MetaKeyBucketSeq(byte slot, int bucketsPerSlot, File slotDir) throws IOException {
        this.slot = slot;
        this.bucketsPerSlot = bucketsPerSlot;

        var file = new File(slotDir, META_KEY_BUCKET_SEQ_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);

            var initTimes = bucketsPerSlot / 1024;
            for (int i = 0; i < initTimes; i++) {
                FileUtils.writeByteArrayToFile(file, EMPTY_BYTES, true);
            }
        }
        this.raf = new RandomAccessFile(file, "rw");
    }

    public synchronized void writeSeq(int bucketIndex, byte splitIndex, long seq) {
        try {
            raf.seek((long) splitIndex * bucketIndex * Long.BYTES);
            raf.writeLong(seq);
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

        try {
            raf.seek((long) splitIndex * beginBucketIndex * Long.BYTES);
            raf.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized long readSeq(int bucketIndex, byte splitIndex) {
        try {
            raf.seek((long) splitIndex * bucketIndex * Long.BYTES);
            return raf.readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record OneSeq(long seq, int bucketIndex, int splitIndex) {
    }

    public synchronized ArrayList<OneSeq> readSeqBatch(int beginBucketIndex, byte splitIndex, int size) {
        var bytes = new byte[size * Long.BYTES];
        try {
            raf.seek((long) splitIndex * beginBucketIndex * Long.BYTES);
            raf.read(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var buffer = ByteBuffer.wrap(bytes);
        var seqList = new ArrayList<OneSeq>();
        for (int i = 0; i < size; i++) {
            seqList.add(new OneSeq(buffer.getLong(), beginBucketIndex + i, splitIndex));
        }
        return seqList;
    }

    public synchronized void clear() {
        var initTimes = bucketsPerSlot / 1024;
        try {
            for (int i = 0; i < initTimes; i++) {
                raf.seek((long) i * BATCH_SIZE);
                raf.write(EMPTY_BYTES);
            }
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
