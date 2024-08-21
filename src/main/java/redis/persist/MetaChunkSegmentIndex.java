package redis.persist;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForGlobal;
import redis.repl.Binlog;
import redis.repl.SlaveNeedReplay;
import redis.repl.SlaveReplay;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class MetaChunkSegmentIndex {
    private static final String META_CHUNK_SEGMENT_INDEX_FILE = "meta_chunk_segment_index.dat";
    private RandomAccessFile raf;

    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public MetaChunkSegmentIndex(byte slot, File slotDir) throws IOException {
        // 4 bytes for chunk segment index int
        // when slave connect master, master start binlog
        // 8 bytes for master uuid long
        // 4 bytes for target master exists data all fetched done flag int
        // 4 bytes for master binlog file index int
        // 8 bytes for master binlog offset long
        this.inMemoryCachedBytes = new byte[4 + 8 + 4 + 4 + 8];

        if (ConfForGlobal.pureMemory) {
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
            ByteBuffer tmpBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            log.warn("Read meta chunk segment index file success, file: {}, slot: {}, segment index: {}, " +
                            "master binlog file index: {}, master binlog offset: {}",
                    file, slot, tmpBuffer.getInt(0), tmpBuffer.getInt(4), tmpBuffer.getLong(8));
        }

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
    }

    @SlaveNeedReplay
    void set(int segmentIndex) {
        if (ConfForGlobal.pureMemory) {
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

    public void setMasterBinlogFileIndexAndOffset(long masterUuid, boolean isExistsDataAllFetched,
                                                  int masterBinlogFileIndexNextTimeToFetch, long masterBinlogOffsetNextTimeToFetch) {
        setAll(get(), masterUuid, isExistsDataAllFetched, masterBinlogFileIndexNextTimeToFetch, masterBinlogOffsetNextTimeToFetch);
    }

    void setAll(int segmentIndex, long masterUuid, boolean isExistsDataAllFetched,
                int masterBinlogFileIndex, long masterBinlogOffset) {
        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer.putInt(0, segmentIndex);
            this.inMemoryCachedByteBuffer.putLong(4, masterUuid);
            this.inMemoryCachedByteBuffer.putInt(12, isExistsDataAllFetched ? 1 : 0);
            this.inMemoryCachedByteBuffer.putInt(16, masterBinlogFileIndex);
            this.inMemoryCachedByteBuffer.putLong(20, masterBinlogOffset);
            return;
        }

        var updatedBytes = new byte[4 + 8 + 4 + 4 + 8];
        ByteBuffer updatedBuffer = ByteBuffer.wrap(updatedBytes);
        updatedBuffer.putInt(segmentIndex);
        updatedBuffer.putLong(masterUuid);
        updatedBuffer.putInt(isExistsDataAllFetched ? 1 : 0);
        updatedBuffer.putInt(masterBinlogFileIndex);
        updatedBuffer.putLong(masterBinlogOffset);
        try {
            raf.seek(0);
            raf.write(updatedBytes);
            inMemoryCachedByteBuffer.position(0).put(updatedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    int get() {
        return inMemoryCachedByteBuffer.getInt(0);
    }

    public long getMasterUuid() {
        return inMemoryCachedByteBuffer.getLong(4);
    }

    public boolean isExistsDataAllFetched() {
        return inMemoryCachedByteBuffer.getInt(12) == 1;
    }

    public Binlog.FileIndexAndOffset getMasterBinlogFileIndexAndOffset() {
        return new Binlog.FileIndexAndOffset(inMemoryCachedByteBuffer.getInt(16), inMemoryCachedByteBuffer.getLong(20));
    }

    @SlaveNeedReplay
    @SlaveReplay
    void clear() {
        setAll(0, 0L, false, 0, 0L);
        System.out.println("Meta chunk segment index clear done, set 0 from the beginning. Clear master binlog file index and offset.");
    }

    void cleanUp() {
        if (ConfForGlobal.pureMemory) {
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
