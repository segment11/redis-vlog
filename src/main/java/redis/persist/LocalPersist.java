package redis.persist;

import com.kenai.jffi.PageManager;
import io.activej.config.Config;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.SnowFlake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class LocalPersist {
    public static final int PAGE_SIZE = (int) PageManager.getInstance().pageSize();
    public static final int PROTECTION = PageManager.PROT_READ | PageManager.PROT_WRITE | PageManager.PROT_EXEC;
    public static final short DEFAULT_SLOT_NUMBER = 4;
    public static final short MAX_SLOT_NUMBER = 128;

    // singleton
    private static final LocalPersist instance = new LocalPersist();

    public static LocalPersist getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(LocalPersist.class);

    private LocalPersist() {
        System.setProperty("jnr.ffi.asm.enabled", "false");
        libC = LibraryLoader.create(LibC.class).load("c");
    }

    private final LibC libC;

    public static final int O_DIRECT = 040000;

    private ChunkMerger chunkMerger;

    public void initChunkMerger(ChunkMerger chunkMerger) throws IOException {
        this.chunkMerger = chunkMerger;
        this.chunkMerger.initTopMergeSegmentIndexMap(libC, persistDir);

        for (var oneSlot : oneSlots) {
            oneSlot.setChunkMerger(chunkMerger);
        }
    }

    public void persistMergeSegmentsUndone() throws Exception {
        for (var oneSlot : oneSlots) {
            oneSlot.persistMergeSegmentsUndone();
        }
    }

    private short slotNumber;
    private File persistDir;
    private OneSlot[] oneSlots;

    public OneSlot oneSlot(byte slot) {
        return oneSlots[slot];
    }

    public long getKeyCount() {
        long count = 0;
        for (var oneSlot : oneSlots) {
            count += oneSlot.getKeyCount();
        }
        return count;
    }

    public int bucketIndex(long keyHash) {
        return (int) Math.abs(keyHash % ConfForSlot.global.confBucket.bucketsPerSlot);
    }

    public void init(byte allWorkers, byte requestWorkers, byte mergeWorkers, byte topMergeWorkers, short slotNumber,
                     SnowFlake snowFlake, File dirFile, Config persistConfig) throws IOException {
        this.slotNumber = slotNumber;

        // already created
        this.persistDir = new File(dirFile, "persist");

        this.oneSlots = new OneSlot[slotNumber];
        for (short i = 0; i < slotNumber; i++) {
            var oneSlot = new OneSlot((byte) i, snowFlake, persistDir, persistConfig);
            oneSlots[i] = oneSlot;
            oneSlot.initChunks(libC, allWorkers, requestWorkers, mergeWorkers, topMergeWorkers);
        }
    }

    public void fixSlotThreadId(byte slot, long threadId) {
        oneSlots[slot].threadIdProtected = threadId;
        log.warn("Fix slot thread id, s={}, tid={}", slot, threadId);
    }

    public void cleanUp() {
        for (var oneSlot : oneSlots) {
            oneSlot.cleanUp();
        }
    }

    public int startChunkMergerJob(byte workerId, byte slot, byte batchIndex, int segmentIndex) throws ExecutionException, InterruptedException {
        ArrayList<Integer> segmentIndexList = new ArrayList<>();
        segmentIndexList.add(segmentIndex);
        var i = chunkMerger.execute(workerId, slot, batchIndex, segmentIndexList).get();
        if (i == -1) {
            throw new IllegalStateException("Merge segment error, w=" + workerId + ", s=" + slot + ", b=" + batchIndex + ", i=" + segmentIndex);
        }
        return i;
    }

    public void flush(byte slot) {
        // should use eventloop submit to make sure thread safe, todo
        oneSlots[slot].flush();
    }
}
