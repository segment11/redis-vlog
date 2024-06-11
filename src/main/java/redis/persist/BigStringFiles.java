package redis.persist;

import io.prometheus.client.Gauge;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BigStringFiles {
    private final byte slot;
    private final String slotStr;
    final File bigStringDir;

    private static final String BIG_STRING_DIR_NAME = "big-string";

    private final LRUMap<Long, byte[]> bigStringBytesByUuidLRU;

    private static final Gauge bigStringFilesCountGauge = Gauge.build()
            .name("big_string_files_count")
            .help("big string files count")
            .labelNames("slot")
            .register();

    private final Logger log = LoggerFactory.getLogger(getClass());

    public BigStringFiles(byte slot, File slotDir) throws IOException {
        this.slot = slot;
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
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.big_string, lruMemoryRequireMB, false);

        this.bigStringBytesByUuidLRU = new LRUMap<>(maxSize);
        bigStringFilesCountGauge.labels(slotStr).set(0);
    }

    public List<Long> getBigStringFileUuidList() {
        var list = new ArrayList<Long>();
        File[] files = bigStringDir.listFiles();
        for (File file : files) {
            list.add(Long.parseLong(file.getName()));
        }
        return list;
    }

    public byte[] getBigStringBytesFromCache(long uuid) {
        var bytesCached = bigStringBytesByUuidLRU.get(uuid);
        if (bytesCached != null) {
            return bytesCached;
        }

        var bytes = readBigStringBytes(uuid);
        if (bytes != null) {
            bigStringBytesByUuidLRU.put(uuid, bytes);
            bigStringFilesCountGauge.labels(slotStr).set(bigStringBytesByUuidLRU.size());
        }
        return bytes;
    }

    private byte[] readBigStringBytes(long uuid) {
        var file = new File(bigStringDir, String.valueOf(uuid));
        if (!file.exists()) {
            log.warn("Big string file not exists, uuid: {}", uuid);
            return null;
        }

        try {
            return FileUtils.readFileToByteArray(file);
        } catch (IOException e) {
            log.error("Read big string file error, uuid: " + uuid, e);
            return null;
        }
    }

    void writeBigStringBytes(long uuid, String key, byte[] bytes) {
        var file = new File(bigStringDir, String.valueOf(uuid));
        try {
            FileUtils.writeByteArrayToFile(file, bytes);
        } catch (IOException e) {
            log.error("Write big string file error, uuid: " + uuid + ", key: " + key, e);
        }
    }

    boolean deleteBigStringFileIfExist(long uuid) {
        bigStringBytesByUuidLRU.remove(uuid);
        bigStringFilesCountGauge.labels(slotStr).set(bigStringBytesByUuidLRU.size());

        var file = new File(bigStringDir, String.valueOf(uuid));
        if (file.exists()) {
            return file.delete();
        } else {
            return true;
        }
    }
}
