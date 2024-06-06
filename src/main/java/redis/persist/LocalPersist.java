package redis.persist;

import com.kenai.jffi.PageManager;
import io.activej.config.Config;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.ConfVolumeDirsForSlot;
import redis.SnowFlake;

import java.io.File;
import java.io.IOException;
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

    public void persistMergeSegmentsUndone() throws ExecutionException, InterruptedException {
        for (var oneSlot : oneSlots) {
            oneSlot.persistMergeSegmentsUndone();
        }
    }

    private short slotNumber;
    private File persistDir;
    private OneSlot[] oneSlots;

    public OneSlot[] oneSlots() {
        return oneSlots;
    }

    public OneSlot oneSlot(byte slot) {
        return oneSlots[slot];
    }

    public int bucketIndex(long keyHash) {
        return (int) Math.abs(keyHash % ConfForSlot.global.confBucket.bucketsPerSlot);
    }

    public void initSlots(byte netWorkers, short slotNumber, SnowFlake[] snowFlakes, File persistDir, Config persistConfig) throws IOException {
        this.slotNumber = slotNumber;
        this.persistDir = persistDir;
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber);

        this.oneSlots = new OneSlot[slotNumber];
        for (short slot = 0; slot < slotNumber; slot++) {
            var i = slot % netWorkers;
            var oneSlot = new OneSlot((byte) slot, slotNumber, snowFlakes[i], persistDir, persistConfig);
            oneSlot.initFds(libC, netWorkers);

            oneSlots[slot] = oneSlot;
        }
    }

    public void debugMode() {
        for (var oneSlot : oneSlots) {
            oneSlot.debugMode();
        }
    }

    public void fixSlotThreadId(byte slot, long threadId) {
        oneSlots[slot].threadIdProtectedWhenPut = threadId;
        log.warn("Fix slot thread id, s={}, tid={}", slot, threadId);
    }

    public void cleanUp() {
        for (var oneSlot : oneSlots) {
            oneSlot.cleanUp();
        }
    }
}
