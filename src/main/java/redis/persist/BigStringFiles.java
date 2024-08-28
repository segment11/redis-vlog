package redis.persist;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForGlobal;
import redis.ConfForSlot;
import redis.metric.SimpleGauge;
import redis.repl.SlaveNeedReplay;
import redis.repl.SlaveReplay;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BigStringFiles implements InMemoryEstimate {
    private final short slot;
    private final String slotStr;
    final File bigStringDir;

    private static final String BIG_STRING_DIR_NAME = "big-string";

    private final LRUMap<Long, byte[]> bigStringBytesByUuidLRU;

    private final HashMap<Long, byte[]> allBytesByUuid = new HashMap<>();

    final static SimpleGauge bigStringFilesCountGauge = new SimpleGauge("big_string_files_count", "big string files count",
            "slot");

    static {
        bigStringFilesCountGauge.register();
    }

    private int bigStringFilesCount = 0;

    private void initMetricsCollect() {
        bigStringFilesCountGauge.addRawGetter(() -> {
            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("big_string_files_count", new SimpleGauge.ValueWithLabelValues((double) bigStringFilesCount,
                    List.of(slotStr)));
            return map;
        });
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public BigStringFiles(short slot, File slotDir) throws IOException {
        this.slot = slot;
        if (ConfForGlobal.pureMemory) {
            log.warn("Pure memory mode, big string files will not be used, slot: {}", slot);
            this.slotStr = null;
            this.bigStringDir = null;
            this.bigStringBytesByUuidLRU = null;
            return;
        }

        this.slotStr = String.valueOf(slot);
        this.bigStringDir = new File(slotDir, BIG_STRING_DIR_NAME);
        if (!bigStringDir.exists()) {
            if (!bigStringDir.mkdirs()) {
                throw new IOException("Create big string dir error, slot: " + slot);
            }
        }

        var maxSize = ConfForSlot.global.lruBigString.maxSize;
        final var maybeOneBigStringBytesLength = 4096;
        var lruMemoryRequireMB = maxSize * maybeOneBigStringBytesLength / 1024 / 1024;
        log.info("LRU max size for big string: {}, maybe one big string bytes length is {}B, memory require: {}MB, slot: {}",
                maxSize,
                maybeOneBigStringBytesLength,
                lruMemoryRequireMB,
                slot);
        log.info("LRU prepare, type: {}, MB: {}, slot: {}", LRUPrepareBytesStats.Type.big_string, lruMemoryRequireMB, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.big_string, slotStr, lruMemoryRequireMB, false);

        this.bigStringBytesByUuidLRU = new LRUMap<>(maxSize);

        var files = bigStringDir.listFiles();
        bigStringFilesCount = files.length;
        initMetricsCollect();
    }

    @Override
    public long estimate() {
        if (ConfForGlobal.pureMemory) {
            return 0;
        }

        long size = 0;
        size += RamUsageEstimator.sizeOfMap(bigStringBytesByUuidLRU);
        return size;
    }

    public List<Long> getBigStringFileUuidList() {
        var list = new ArrayList<Long>();
        if (ConfForGlobal.pureMemory) {
            for (var entry : allBytesByUuid.entrySet()) {
                list.add(entry.getKey());
            }
            return list;
        }

        var files = bigStringDir.listFiles();
        for (File file : files) {
            list.add(Long.parseLong(file.getName()));
        }
        return list;
    }

    public byte[] getBigStringBytes(long uuid) {
        return getBigStringBytes(uuid, false);
    }

    public byte[] getBigStringBytes(long uuid, boolean doLRUCache) {
        if (ConfForGlobal.pureMemory) {
            return allBytesByUuid.get(uuid);
        }

        var bytesCached = bigStringBytesByUuidLRU.get(uuid);
        if (bytesCached != null) {
            return bytesCached;
        }

        var bytes = readBigStringBytes(uuid);
        if (bytes != null && doLRUCache) {
            bigStringBytesByUuidLRU.put(uuid, bytes);
        }
        return bytes;
    }

    private byte[] readBigStringBytes(long uuid) {
        var file = new File(bigStringDir, String.valueOf(uuid));
        if (!file.exists()) {
            log.warn("Big string file not exists, uuid: {}, slot: {}", uuid, slot);
            return null;
        }

        try {
            return FileUtils.readFileToByteArray(file);
        } catch (IOException e) {
            log.error("Read big string file error, uuid: " + uuid + ", slot: " + slot, e);
            return null;
        }
    }

    public boolean writeBigStringBytes(long uuid, String key, byte[] bytes) {
        if (ConfForGlobal.pureMemory) {
            allBytesByUuid.put(uuid, bytes);
            return true;
        }

        var file = new File(bigStringDir, String.valueOf(uuid));
        try {
            FileUtils.writeByteArrayToFile(file, bytes);
            bigStringFilesCount++;
            return true;
        } catch (IOException e) {
            log.error("Write big string file error, uuid: " + uuid + ", key: " + key + ", slot: " + slot, e);
            return false;
        }
    }

    public boolean deleteBigStringFileIfExist(long uuid) {
        if (ConfForGlobal.pureMemory) {
            allBytesByUuid.remove(uuid);
            return true;
        }

        bigStringBytesByUuidLRU.remove(uuid);
        bigStringFilesCount--;

        var file = new File(bigStringDir, String.valueOf(uuid));
        if (file.exists()) {
            return file.delete();
        } else {
            return true;
        }
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void deleteAllBigStringFiles() {
        if (ConfForGlobal.pureMemory) {
            allBytesByUuid.clear();
            return;
        }

        bigStringBytesByUuidLRU.clear();
        bigStringFilesCount = 0;

        try {
            FileUtils.cleanDirectory(bigStringDir);
            log.warn("Delete all big string files, slot: {}", slot);
        } catch (IOException e) {
            log.error("Delete all big string files error, slot: " + slot, e);
        }
    }
}
