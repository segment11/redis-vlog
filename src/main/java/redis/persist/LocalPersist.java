package redis.persist;

import com.kenai.jffi.PageManager;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfVolumeDirsForSlot;
import redis.SnowFlake;

import java.io.File;
import java.io.IOException;

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

    public void persistMergeSegmentsUndone() {
        for (var oneSlot : oneSlots) {
            oneSlot.persistMergingOrMergedSegmentsButNotPersisted();
            // merged segments may do merge again, it is ok, only do once when server restart
            oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(true);
            oneSlot.getMergedSegmentIndexEndLastTime();
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

    public void addOneSlotForTest(byte slot, Eventloop eventloop) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        var oneSlot = new OneSlot(slot, eventloop);
        oneSlot.threadIdProtectedForSafe = eventloop.getEventloopThread().threadId();
        this.oneSlots = new OneSlot[slot + 1];
        this.oneSlots[slot] = oneSlot;
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
        oneSlots[slot].threadIdProtectedForSafe = threadId;
        log.warn("Fix slot thread id, s={}, tid={}", slot, threadId);
    }

    public OneSlot currentThreadFirstOneSlot() {
        for (int i = 0; i < oneSlots.length; i++) {
            var oneSlot = oneSlots[i];
            if (oneSlot.threadIdProtectedForSafe == Thread.currentThread().threadId()) {
                return oneSlot;
            }
        }
        throw new IllegalStateException("No one slot for current thread");
    }

    public void cleanUp() {
        for (var oneSlot : oneSlots) {
            oneSlot.asyncRun(oneSlot::cleanUp);
        }
    }
}
