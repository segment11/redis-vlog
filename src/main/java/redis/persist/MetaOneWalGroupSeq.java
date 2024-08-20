package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.repl.SlaveNeedReplay;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

// for slave check if key buckets need fetch from master, compare with seq
public class MetaOneWalGroupSeq implements InMemoryEstimate {
    private static final String META_ONE_WAL_GROUP_SEQ_FILE = "meta_one_wal_group_seq.dat";

    final int walGroupNumber;
    final int allCapacity;
    private RandomAccessFile raf;

    private final byte[] inMemoryCachedBytes;

    private final ByteBuffer inMemoryCachedByteBuffer;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaOneWalGroupSeq(byte slot, File slotDir) throws IOException {
        this.walGroupNumber = Wal.calcWalGroupNumber();
        // 8 bytes long seq for each one wal group, each split index
        this.allCapacity = 8 * walGroupNumber * KeyLoader.MAX_SPLIT_NUMBER;

        // max 8 * 512K / 16 * 9 = 2304KB
        this.inMemoryCachedBytes = new byte[allCapacity];

        if (ConfForSlot.global.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(slotDir, META_ONE_WAL_GROUP_SEQ_FILE);
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
            log.warn("Read one wal group seq file success, file: {}, slot: {}, all capacity: {}KB",
                    file, slot, allCapacity / 1024);

            var sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                sb.append(inMemoryCachedBytes[i]).append(", ");
            }
            log.info("For debug: first 10 one wal group seq: [{}]", sb);
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    @Override
    public long estimate() {
        return allCapacity;
    }

    long get(int oneWalGroupIndex, byte splitIndex) {
        var offset = 8 * oneWalGroupIndex + 8 * walGroupNumber * splitIndex;
        return inMemoryCachedByteBuffer.getLong(offset);
    }

    @SlaveNeedReplay
    void set(int oneWalGroupIndex, byte splitIndex, long seq) {
        var offset = 8 * oneWalGroupIndex + 8 * walGroupNumber * splitIndex;
        if (ConfForSlot.global.pureMemory) {
            inMemoryCachedByteBuffer.putLong(offset, seq);
            return;
        }

        try {
            raf.seek(offset);
            raf.writeLong(seq);
            inMemoryCachedByteBuffer.putLong(offset, seq);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void clear() {
        if (ConfForSlot.global.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            return;
        }

        try {
            var tmpBytes = new byte[allCapacity];
            raf.seek(0);
            raf.write(tmpBytes);
            inMemoryCachedByteBuffer.position(0).put(tmpBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanUp() {
        if (ConfForSlot.global.pureMemory) {
            return;
        }

        // sync all
        try {
//            raf.getFD().sync();
//            System.out.println("Meta one wal group seq sync all done");
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
