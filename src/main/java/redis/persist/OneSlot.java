package redis.persist;

import com.github.luben.zstd.Zstd;
import io.activej.async.callback.AsyncComputation;
import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import jnr.posix.LibC;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.*;
import redis.metric.SimpleGauge;
import redis.repl.*;
import redis.repl.content.RawBytesContent;
import redis.repl.incremental.XBigStrings;
import redis.repl.incremental.XOneWalGroupPersist;
import redis.repl.incremental.XWalV;
import redis.task.ITask;
import redis.task.TaskChain;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import static io.activej.config.converter.ConfigConverters.ofBoolean;
import static redis.persist.Chunk.*;
import static redis.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;

public class OneSlot {
    // for unit test
    public OneSlot(byte slot, File slotDir, KeyLoader keyLoader, Wal wal) throws IOException {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = slotDir;
        this.slotNumber = 1;

        this.keyLoader = keyLoader;
        this.snowFlake = new SnowFlake(1, 1);
        this.chunkSegmentLength = 4096;

        this.bigStringFiles = new BigStringFiles(slot, slotDir);
        this.chunkMergeWorker = null;
        this.dynConfig = null;
        this.walGroupNumber = 1;
        this.walArray = new Wal[]{wal};
        this.raf = null;
        this.rafShortValue = null;
        this.masterUuid = 0L;

        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);

        this.binlog = null;
    }

    // for unit test, only for local persist one slot array
    OneSlot(byte slot) {
        this(slot, null);
    }

    // for unit test, only for async run/call
    OneSlot(byte slot, Eventloop eventloop) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = null;
        this.slotNumber = 1;

        this.keyLoader = null;
        this.snowFlake = null;
        this.chunkSegmentLength = 4096;

        this.bigStringFiles = null;
        this.chunkMergeWorker = null;
        this.dynConfig = null;
        this.walGroupNumber = 1;
        this.walArray = new Wal[0];
        this.raf = null;
        this.rafShortValue = null;
        this.masterUuid = 0L;

        this.metaChunkSegmentFlagSeq = null;
        this.metaChunkSegmentIndex = null;

        this.binlog = null;

        this.netWorkerEventloop = eventloop;
    }

    public OneSlot(byte slot, short slotNumber, SnowFlake snowFlake, File persistDir, Config persistConfig) throws IOException {
        this.chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;

        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotNumber = slotNumber;
        this.snowFlake = snowFlake;

        var volumeDirPath = ConfVolumeDirsForSlot.getVolumeDirBySlot(slot);
        if (volumeDirPath != null) {
            // already exists
            var volumeDir = new File(volumeDirPath);
            this.slotDir = new File(volumeDir, "slot-" + slot);
        } else {
            this.slotDir = new File(persistDir, "slot-" + slot);
        }

        if (!slotDir.exists()) {
            if (!slotDir.mkdirs()) {
                throw new IOException("Create slot dir error, slot: " + slot);
            }
        }

        this.bigStringFiles = new BigStringFiles(slot, slotDir);

        this.chunkMergeWorker = new ChunkMergeWorker(slot, this);

        var dynConfigFile = new File(slotDir, DYN_CONFIG_FILE_NAME);
        this.dynConfig = new DynConfig(slot, dynConfigFile);

        var masterUuidSaved = dynConfig.getMasterUuid();
        if (masterUuidSaved != null) {
            this.masterUuid = masterUuidSaved;
        } else {
            this.masterUuid = snowFlake.nextId();
            dynConfig.setMasterUuid(masterUuid);
        }

        this.dynConfig.setBinlogOn(persistConfig.get(ofBoolean(), "binlogOn", false));
        log.warn("Binlog on: {}", this.dynConfig.isBinlogOn());

        this.walGroupNumber = Wal.calcWalGroupNumber();
        this.walArray = new Wal[walGroupNumber];
        log.info("One slot wal group number: {}, slot: {}", walGroupNumber, slot);

        this.chunkMergeWorker.resetThreshold(walGroupNumber);

        var walSharedFile = new File(slotDir, "wal.dat");
        if (!walSharedFile.exists()) {
            FileUtils.touch(walSharedFile);

            var initTimes = walGroupNumber / Wal.GROUP_COUNT_IN_M4;
            for (int j = 0; j < initTimes; j++) {
                FileUtils.writeByteArrayToFile(walSharedFile, Wal.INIT_M4, true);
            }
        }
        this.raf = new RandomAccessFile(walSharedFile, "rw");
        var lruMemoryRequireMBWriteInWal = walSharedFile.length() / 1024 / 1024;
        log.info("LRU prepare, type: {}, MB: {}, slot: {}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, (int) lruMemoryRequireMBWriteInWal, false);

        var walSharedFileShortValue = new File(slotDir, "wal-short-value.dat");
        if (!walSharedFileShortValue.exists()) {
            FileUtils.touch(walSharedFileShortValue);

            var initTimes = walGroupNumber / Wal.GROUP_COUNT_IN_M4;
            for (int j = 0; j < initTimes; j++) {
                FileUtils.writeByteArrayToFile(walSharedFileShortValue, Wal.INIT_M4, true);
            }
        }
        this.rafShortValue = new RandomAccessFile(walSharedFileShortValue, "rw");
        var lruMemoryRequireMBWriteInWal2 = walSharedFileShortValue.length() / 1024 / 1024;
        log.info("LRU prepare, type: {}, short value, MB: {}, slot: {}", LRUPrepareBytesStats.Type.kv_write_in_wal, lruMemoryRequireMBWriteInWal2, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, (int) lruMemoryRequireMBWriteInWal2, false);

        long initMemoryN = 0;
        for (int i = 0; i < walGroupNumber; i++) {
            var wal = new Wal(slot, i, raf, rafShortValue, snowFlake);
            walArray[i] = wal;
            initMemoryN += wal.initMemoryN;
        }

        int initMemoryMB = (int) (initMemoryN / 1024 / 1024);
        log.info("Static memory init, type: {}, MB: {}, slot: {}", StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, slot);
        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.wal_cache_init, initMemoryMB, false);

        // cache lru
        int maxSizeForAllWalGroups = ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize;
        var maxSizeForEachWalGroup = maxSizeForAllWalGroups / walGroupNumber;
        final var maybeOneCompressedValueEncodedLength = 200;
        var lruMemoryRequireMBReadGroupByWalGroup = maxSizeForAllWalGroups * maybeOneCompressedValueEncodedLength / 1024 / 1024;
        log.info("LRU max size for each wal group: {}, all wal group number: {}, maybe one compressed value encoded length is {}B, memory require: {}MB, slot: {}",
                maxSizeForEachWalGroup,
                walGroupNumber,
                maybeOneCompressedValueEncodedLength,
                lruMemoryRequireMBReadGroupByWalGroup,
                slot);
        log.info("LRU prepare, type: {}, MB: {}, slot: {}", LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, lruMemoryRequireMBReadGroupByWalGroup, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, lruMemoryRequireMBReadGroupByWalGroup, false);

        for (int walGroupIndex = 0; walGroupIndex < walGroupNumber; walGroupIndex++) {
            LRUMap<String, byte[]> lru = new LRUMap<>(maxSizeForEachWalGroup);
            kvByWalGroupIndexLRU.put(walGroupIndex, lru);
        }

        this.keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, this);

        this.binlog = new Binlog(slot, slotDir, dynConfig);
        // only set slot 0, binlog, if current instance do not include slot 0, need change here
        if (this.slot == 0) {
            DictMap.getInstance().setBinlog(this.binlog);
        }

        this.initTasks();
        this.initMetricsCollect();
    }

    @Override
    public String toString() {
        return "OneSlot{" +
                "slot=" + slot +
                ", slotNumber=" + slotNumber +
                ", slotDir=" + slotDir +
                '}';
    }

    private final Logger log = LoggerFactory.getLogger(OneSlot.class);

    public long getMasterUuid() {
        return masterUuid;
    }

    private final long masterUuid;

    final ArrayList<ReplPair> replPairs = new ArrayList<>();

    // slave need not top merge
    public boolean isAsSlave() {
        boolean isAsSlave = false;
        for (var replPair : replPairs) {
            if (!replPair.isSendBye() && !replPair.isAsMaster()) {
                isAsSlave = true;
                break;
            }
        }
        return isAsSlave;
    }

    final LinkedList<ReplPair> delayNeedCloseReplPairs = new LinkedList<>();

    public void addDelayNeedCloseReplPair(ReplPair replPair) {
        delayNeedCloseReplPairs.add(replPair);
    }

    // todo, both master - master, need change equal and init as master or slave
    public ReplPair createReplPairAsSlave(String host, int port) throws IOException {
        var replPair = new ReplPair(slot, false, host, port);
        replPair.setSlaveUuid(masterUuid);
        replPair.initAsSlave(netWorkerEventloop, requestHandler);
        log.warn("Create repl pair as slave, host: {}, port: {}, slot: {}", host, port, slot);
        replPairs.add(replPair);

        if (!isReadonly()) {
            setReadonly(true);
        }
        if (isCanRead()) {
            setCanRead(false);
        }
        return replPair;
    }

    public void removeReplPairAsSlave() throws IOException {
        for (var replPair : replPairs) {
            if (replPair.isAsMaster()) {
                continue;
            }

            if (replPair.isSendBye()) {
                continue;
            }

            replPair.bye();
            addDelayNeedCloseReplPair(replPair);
        }

        if (isReadonly()) {
            setReadonly(false);
        }
        if (!isCanRead()) {
            setCanRead(true);
        }
    }

    public ReplPair getReplPairAsMaster(long slaveUuid) {
        for (var replPair : replPairs) {
            if (!replPair.isAsMaster()) {
                continue;
            }

            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.getSlaveUuid() != slaveUuid) {
                continue;
            }

            return replPair;
        }
        return null;
    }

    public ReplPair getReplPairAsSlave(long slaveUuid) {
        for (var replPair : replPairs) {
            if (replPair.isAsMaster()) {
                continue;
            }

            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.getSlaveUuid() != slaveUuid) {
                continue;
            }

            return replPair;
        }
        return null;
    }

    public ReplPair createIfNotExistReplPairAsMaster(long slaveUuid, String host, int port) {
        var replPair = new ReplPair(slot, true, host, port);
        replPair.setSlaveUuid(slaveUuid);
        replPair.setMasterUuid(masterUuid);

        for (var replPair1 : replPairs) {
            if (replPair1.equals(replPair)) {
                log.warn("Repl pair as master already exists, host: {}, port: {}, slot: {}", host, port, slot);
                return replPair1;
            }
        }

        log.warn("Create repl pair as master, host: {}, port: {}, slot: {}", host, port, slot);
        replPairs.add(replPair);
        return replPair;
    }

    public void setNetWorkerEventloop(Eventloop netWorkerEventloop) {
        this.netWorkerEventloop = netWorkerEventloop;
    }

    private Eventloop netWorkerEventloop;

    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private RequestHandler requestHandler;

    public Promise<Void> asyncRun(RunnableEx runnableEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                runnableEx.run();
                return Promise.complete();
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(netWorkerEventloop.submit(runnableEx));
    }

    public <T> Promise<T> asyncCall(SupplierEx<T> supplierEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                return Promise.of(supplierEx.get());
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(netWorkerEventloop.submit(AsyncComputation.of(supplierEx)));
    }

    public void delayRun(int millis, Runnable runnable) {
        // for unit test
        if (netWorkerEventloop == null) {
            return;
        }

        netWorkerEventloop.delay(millis, runnable);
    }

    private final byte slot;
    private final String slotStr;
    private final short slotNumber;

    public byte slot() {
        return slot;
    }

    private final int chunkSegmentLength;
    final SnowFlake snowFlake;
    final File slotDir;

    private final BigStringFiles bigStringFiles;

    public BigStringFiles getBigStringFiles() {
        return bigStringFiles;
    }

    public File getBigStringDir() {
        return bigStringFiles.bigStringDir;
    }

    private final Map<Integer, LRUMap<String, byte[]>> kvByWalGroupIndexLRU = new HashMap<>();

    int kvByWalGroupIndexCountTotal() {
        int n = 0;
        for (var lru : kvByWalGroupIndexLRU.values()) {
            n += lru.size();
        }
        return n;
    }

    int lruClearedCount = 0;

    int clearKvLRUByWalGroupIndex(int walGroupIndex) {
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        if (lru == null) {
            return 0;
        }

        int n = lru.size();
        lru.clear();
        if (walGroupIndex == 0) {
            lruClearedCount++;
            if (lruClearedCount % 10 == 0) {
                log.info("KV LRU cleared for wal group index: {}, I am alive, act normal", walGroupIndex);
            }
        }
        return n;
    }

    long kvLRUHitTotal = 0;
    private long kvLRUMissTotal = 0;
    private long kvLRUCvEncodedLengthTotal = 0;

    final ChunkMergeWorker chunkMergeWorker;

    private static final String DYN_CONFIG_FILE_NAME = "dyn-config.json";

    private final DynConfig dynConfig;

    public DynConfig getDynConfig() {
        return dynConfig;
    }

    private static final ArrayList<String> dynConfigKeyWhiteList = new ArrayList<>();

    static {
        // add white list here, todo, refer command config in CGroup
        dynConfigKeyWhiteList.add("testKey");
        dynConfigKeyWhiteList.add("testKey2");
    }

    public boolean updateDynConfig(String key, byte[] configValueBytes) throws IOException {
        // check key white list
        if (!dynConfigKeyWhiteList.contains(key)) {
            log.warn("Update dyn config key not in white list, key: {}, slot: {}", key, slot);
            return false;
        }

        if (key.equals("testKey")) {
            dynConfig.setTestKey(Integer.parseInt(new String(configValueBytes)));
            return true;
            // add else if here
        } else {
            log.warn("Update dyn config key not match, key: {}, slot: {}", key, slot);
            return false;
        }
    }

    public boolean isReadonly() {
        return dynConfig.isReadonly();
    }

    public void setReadonly(boolean readonly) throws IOException {
        dynConfig.setReadonly(readonly);
    }

    public boolean isCanRead() {
        return dynConfig.isCanRead();
    }

    public void setCanRead(boolean canRead) throws IOException {
        dynConfig.setCanRead(canRead);
    }

    private final int walGroupNumber;
    // index is group index
    private final Wal[] walArray;

    public Wal getWalByBucketIndex(int bucketIndex) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        return walArray[walGroupIndex];
    }

    public Wal getWalByGroupIndex(int walGroupIndex) {
        return walArray[walGroupIndex];
    }

    private final RandomAccessFile raf;
    private final RandomAccessFile rafShortValue;

    final KeyLoader keyLoader;

    public KeyLoader getKeyLoader() {
        return keyLoader;
    }

    public long getWalKeyCount() {
        long r = 0;
        for (var wal : walArray) {
            r += wal.getKeyCount();
        }
        return r;
    }

    public long getAllKeyCount() {
        // for unit test
        if (keyLoader == null) {
            return 0;
        }
        return keyLoader.getKeyCount() + getWalKeyCount();
    }

    private LibC libC;
    Chunk chunk;

    public Chunk getChunk() {
        return chunk;
    }

    MetaChunkSegmentFlagSeq metaChunkSegmentFlagSeq;

    public MetaChunkSegmentFlagSeq getMetaChunkSegmentFlagSeq() {
        return metaChunkSegmentFlagSeq;
    }

    MetaChunkSegmentIndex metaChunkSegmentIndex;

    public MetaChunkSegmentIndex getMetaChunkSegmentIndex() {
        return metaChunkSegmentIndex;
    }

    int getChunkWriteSegmentIndex() {
        return metaChunkSegmentIndex.get();
    }

    public void setMetaChunkSegmentIndex(int segmentIndex) {
        setMetaChunkSegmentIndex(segmentIndex, false);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void setMetaChunkSegmentIndex(int segmentIndex, boolean updateChunkSegmentIndex) {
        if (segmentIndex < 0 || segmentIndex > chunk.maxSegmentIndex) {
            throw new IllegalArgumentException("Segment index out of bound, s=" + slot + ", i=" + segmentIndex);
        }

        metaChunkSegmentIndex.set(segmentIndex);
        if (updateChunkSegmentIndex) {
            chunk.segmentIndex = segmentIndex;
        }
    }

    public void setChunkSegmentIndexFromMeta() {
        chunk.segmentIndex = metaChunkSegmentIndex.get();
    }

    private final Binlog binlog;

    public Binlog getBinlog() {
        return binlog;
    }

    public void appendBinlog(BinlogContent content) {
        if (binlog != null) {
            try {
                binlog.append(content);
            } catch (IOException e) {
                throw new RuntimeException("Append binlog error, slot: " + slot, e);
            }
        }
    }

    private final TaskChain taskChain = new TaskChain();

    public TaskChain getTaskChain() {
        return taskChain;
    }

    public void doTask(int loopCount) {
        taskChain.doTask(loopCount);
    }

    private void initTasks() {
        taskChain.add(new ITask() {
            private int loopCount = 0;

            @Override
            public String name() {
                return "repl pair slave ping";
            }

            @Override
            public void run() {
                if (loopCount % 1000 == 0) {
                    log.info("Task {} run, slot: {}, loop count: {}", name(), slot, loopCount);
                }

                for (var replPair : replPairs) {
                    if (replPair.isSendBye()) {
                        continue;
                    }

                    if (!replPair.isAsMaster()) {
                        // only slave need send ping
                        replPair.ping();

                        var toFetchBigStringUuids = replPair.doingFetchBigStringUuid();
                        if (toFetchBigStringUuids != -1) {
                            var bytes = new byte[8];
                            ByteBuffer.wrap(bytes).putLong(toFetchBigStringUuids);
                            replPair.write(ReplType.incremental_big_string, new RawBytesContent(bytes));
                            log.info("Repl do fetch incremental big string, to server: {}, slot: {}, uuid: {}",
                                    replPair.getHostAndPort(), slot, toFetchBigStringUuids);
                        }
                    }
                }

                if (!delayNeedCloseReplPairs.isEmpty()) {
                    var needCloseReplPair = delayNeedCloseReplPairs.pop();
                    needCloseReplPair.close();

                    var it = replPairs.iterator();
                    while (it.hasNext()) {
                        var replPair = it.next();
                        if (replPair.equals(needCloseReplPair)) {
                            it.remove();
                            log.warn("Remove repl pair after bye, to server: {}, slot: {}", replPair.getHostAndPort(), slot);
                            break;
                        }
                    }
                }
            }

            @Override
            public void setLoopCount(int loopCount) {
                this.loopCount = loopCount;
            }
        });
    }

    void debugMode() {
        taskChain.add(new ITask() {
            private int loopCount = 0;

            @Override
            public String name() {
                return "debug";
            }

            @Override
            public void run() {
                log.info("Debug task run, slot: {}, loop count: {}", slot, loopCount);
            }

            @Override
            public void setLoopCount(int loopCount) {
                this.loopCount = loopCount;
            }

            @Override
            public int executeOnceAfterLoopCount() {
                return 10;
            }
        });
    }

    private void checkCurrentThreadId() {
        var threadId = Thread.currentThread().threadId();
        if (threadId != threadIdProtectedForSafe) {
            throw new IllegalStateException("Thread id not match, thread id: " + threadId + ", thread id protected for safe: " + threadIdProtectedForSafe);
        }
    }

    public Long getExpireAt(byte[] keyBytes, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var key = new String(keyBytes);
        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            // write batch kv is the newest
            if (CompressedValue.isDeleted(cvEncodedFromWal)) {
                return null;
            }
            var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncodedFromWal), keyBytes, keyHash);
            return cv.getExpireAt();
        }

        // from lru cache
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        var cvEncodedBytesFromLRU = lru.get(key);
        if (cvEncodedBytesFromLRU != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;

            var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), keyBytes, keyHash);
            return cv.getExpireAt();
        }
        kvLRUMissTotal++;

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, keyBytes, keyHash);
        if (valueBytesWithExpireAtAndSeq == null) {
            return null;
        }

        return valueBytesWithExpireAtAndSeq.expireAt();
    }

    public record BufOrCompressedValue(ByteBuf buf, CompressedValue cv) {
    }

    public BufOrCompressedValue get(byte[] keyBytes, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var key = new String(keyBytes);
        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            // write batch kv is the newest
            if (CompressedValue.isDeleted(cvEncodedFromWal)) {
                return null;
            }
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedFromWal), null);
        }

        // from lru cache
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        var cvEncodedBytesFromLRU = lru.get(key);
        if (cvEncodedBytesFromLRU != null) {
            kvLRUHitTotal++;
            kvLRUCvEncodedLengthTotal += cvEncodedBytesFromLRU.length;
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
        }
        kvLRUMissTotal++;

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, keyBytes, keyHash);
        if (valueBytesWithExpireAtAndSeq == null) {
            return null;
        }

        var valueBytes = valueBytesWithExpireAtAndSeq.valueBytes();
        if (!PersistValueMeta.isPvm(valueBytes)) {
            // short value, just return, CompressedValue can decode
            lru.put(key, valueBytes);
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(valueBytes), null);
        }

        var pvm = PersistValueMeta.decode(valueBytes);
        var decompressedBytes = getSegmentSubBlockDecompressedBytesByPvm(pvm);
//        SegmentBatch.iterateFromSegmentBytesForDebug(decompressedBytes);

        var buf = Unpooled.wrappedBuffer(decompressedBytes);
        // crc check
//        var segmentSeq = buf.readLong();
//        var cvCount = buf.readInt();
//        var segmentMaskedValue = buf.readInt();
//        buf.skipBytes(SEGMENT_HEADER_LENGTH);

        buf.readerIndex(pvm.segmentOffset);

        // skip key header or check key
        var keyLength = buf.readShort();
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length: " + keyLength);
        }

        var keyBytesRead = new byte[keyLength];
        buf.readBytes(keyBytesRead);

        if (!Arrays.equals(keyBytesRead, keyBytes)) {
            throw new IllegalStateException("Key not match, key: " + new String(keyBytes) + ", key persisted: " + new String(keyBytesRead));
        }

        // set to lru cache, just target bytes
        var cv = CompressedValue.decode(buf, keyBytes, keyHash);
        lru.put(key, cv.encode());

        return new BufOrCompressedValue(null, cv);
    }

    byte[] getFromWal(String key, int bucketIndex) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        return targetWal.get(key);
    }

    private byte[] getSegmentSubBlockDecompressedBytesByPvm(PersistValueMeta pvm) {
        byte[] tightBytesWithLength = chunk.preadOneSegment(pvm.segmentIndex);
        if (tightBytesWithLength == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm: " + pvm);
        }

        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        buffer.position(SegmentBatch.subBlockMetaPosition(pvm.subBlockIndex));
        var subBlockOffset = buffer.getShort();
        var subBlockLength = buffer.getShort();

        if (subBlockOffset == 0) {
            throw new IllegalStateException("Sub block offset is 0, pvm: " + pvm);
        }

        var decompressedBytes = new byte[chunkSegmentLength];

        var beginT = System.nanoTime();
        var d = Zstd.decompressByteArray(decompressedBytes, 0, chunkSegmentLength,
                tightBytesWithLength, subBlockOffset, subBlockLength);
        var costT = (System.nanoTime() - beginT) / 1000;
        segmentDecompressTimeTotalUs += costT;
        segmentDecompressCountTotal++;

        if (d != chunkSegmentLength) {
            throw new IllegalStateException("Decompress segment sub block error, s=" + pvm.slot +
                    ", i=" + pvm.segmentIndex + ", sbi=" + pvm.subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunkSegmentLength);
        }

        return decompressedBytes;
    }

    public boolean exists(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var cvEncodedFromWal = getFromWal(key, bucketIndex);
        if (cvEncodedFromWal != null) {
            // write batch kv is the newest
            return !CompressedValue.isDeleted(cvEncodedFromWal);
        }

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, key.getBytes(), keyHash);
        return valueBytesWithExpireAtAndSeq != null && !valueBytesWithExpireAtAndSeq.isExpired();
    }

    public boolean remove(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        if (exists(key, bucketIndex, keyHash)) {
            removeDelay(key, bucketIndex, keyHash);
            return true;
        } else {
            return false;
        }
    }

    @SlaveNeedReplay
    public void removeDelay(String key, int bucketIndex, long keyHash) {
        checkCurrentThreadId();

        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        var putResult = targetWal.removeDelay(key, bucketIndex, keyHash);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, putResult);
        } else {
            var xWalV = new XWalV(putResult.needPutV(), putResult.isValueShort(), putResult.offset());
            appendBinlog(xWalV);
        }
    }

    long threadIdProtectedForSafe = -1;

    public void put(String key, int bucketIndex, CompressedValue cv) {
        put(key, bucketIndex, cv, false);
    }

    @SlaveNeedReplay
    // thread safe, same slot, same event loop
    public void put(String key, int bucketIndex, CompressedValue cv, boolean isFromMerge) {
        checkCurrentThreadId();

        // before put check for better performance, todo
        if (isReadonly()) {
            throw new ReadonlyException();
        }

        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];

        byte[] cvEncoded;
        boolean isValueShort = cv.noExpire() && (cv.isTypeNumber() || cv.isShortString());
        if (isValueShort) {
            if (cv.isTypeNumber()) {
                cvEncoded = cv.encodeAsNumber();
            } else {
                cvEncoded = cv.encodeAsShortString();
            }
        } else {
            cvEncoded = cv.encode();
        }
        var v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                key, cvEncoded, isFromMerge);

        // for big string, use single file
        boolean isPersistLengthOverSegmentLength = v.persistLength() + SEGMENT_HEADER_LENGTH > chunkSegmentLength;
        if (isPersistLengthOverSegmentLength || key.startsWith("kerry-test-big-string-")) {
            var uuid = snowFlake.nextId();
            var bytes = cv.getCompressedData();
            var isWriteOk = bigStringFiles.writeBigStringBytes(uuid, key, bytes);
            if (!isWriteOk) {
                throw new RuntimeException("Write big string file error, uuid: " + uuid + ", key: " + key);
            }

            // encode again
            cvEncoded = cv.encodeAsBigStringMeta(uuid);
            var xBigStrings = new XBigStrings(uuid, key, cvEncoded);
            appendBinlog(xBigStrings);

            v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cvEncoded, isFromMerge);

            isValueShort = true;
        }

        var putResult = targetWal.put(isValueShort, key, v);
        if (!putResult.needPersist()) {
            var xWalV = new XWalV(v, isValueShort, putResult.offset());
            appendBinlog(xWalV);

            return;
        }

        doPersist(walGroupIndex, key, putResult);
    }

    @SlaveNeedReplay
    private void doPersist(int walGroupIndex, String key, Wal.PutResult putResult) {
        var targetWal = walArray[walGroupIndex];
        persistWal(putResult.isValueShort(), targetWal);

        if (putResult.isValueShort()) {
            targetWal.clearShortValues();
        } else {
            targetWal.clearValues();
        }

        var needPutV = putResult.needPutV();
        if (needPutV != null) {
            targetWal.put(putResult.isValueShort(), key, needPutV);

            var xWalV = new XWalV(needPutV, putResult.isValueShort(), putResult.offset());
            appendBinlog(xWalV);
        }
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void flush() {
        checkCurrentThreadId();

        // can truncate all batch for better perf, todo
        for (var wal : walArray) {
            wal.clear();
        }

        if (this.keyLoader != null) {
            try {
                this.keyLoader.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (this.metaChunkSegmentFlagSeq != null) {
            this.metaChunkSegmentFlagSeq.clear();
        }
        if (this.metaChunkSegmentIndex != null) {
            this.metaChunkSegmentIndex.clear();
        }

        if (this.chunk != null) {
            this.chunk.segmentIndex = 0;
        }
    }

    void initFds(LibC libC) throws IOException {
        this.libC = libC;
        this.keyLoader.initFds(libC);

        // meta data
        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);

        // chunk
        initChunk();
    }

    private void initChunk() throws IOException {
        this.chunk = new Chunk(slot, slotDir, this, snowFlake, keyLoader);
        chunk.initFds(libC);

        var segmentIndexLastSaved = getChunkWriteSegmentIndex();

        // write index mmap crash recovery
        boolean isBreak = false;
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT; i++) {
            boolean canWrite = chunk.initSegmentIndexWhenFirstStart(segmentIndexLastSaved + i);
            int currentSegmentIndex = chunk.currentSegmentIndex();
            log.warn("Move segment to write, s={}, i={}", slot, currentSegmentIndex);

            // when restart server, set persisted flag
            if (!canWrite) {
                log.warn("Segment can not write, s={}, i={}", slot, currentSegmentIndex);

                // set persisted flag reuse_new -> need merge before use
                updateSegmentMergeFlag(currentSegmentIndex, Flag.reuse_new, snowFlake.nextId());
                log.warn("Reset segment persisted when init");

                setMetaChunkSegmentIndex(currentSegmentIndex);
            } else {
                setMetaChunkSegmentIndex(currentSegmentIndex);
                isBreak = true;
                break;
            }
        }

        if (!isBreak) {
            throw new IllegalStateException("Segment can not write after reset flag, s=" + slot + ", i=" + chunk.currentSegmentIndex());
        }
    }

    byte[] preadForMerge(int segmentIndex, int segmentCount) {
        checkCurrentThreadId();

        return chunk.preadForMerge(segmentIndex, segmentCount);
    }

    public byte[] preadForRepl(int segmentIndex) {
        checkCurrentThreadId();

        return chunk.preadForRepl(segmentIndex);
    }

    @SlaveReplay
    public void writeChunkSegmentsFromMasterExists(byte[] bytes, int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        if (bytes.length != chunk.chunkSegmentLength * segmentCount) {
            throw new IllegalStateException("Repl write chunk segments bytes length not match, bytes length: " + bytes.length +
                    ", chunk segment length: " + chunk.chunkSegmentLength + ", segment count: " + segmentCount);
        }

        chunk.writeSegmentsFromMasterExists(bytes, beginSegmentIndex, segmentCount);
        log.warn("Repl write chunk segments from master exists, s={}, i={}, c={}", slot, beginSegmentIndex, segmentCount);
    }

    public void cleanUp() {
        checkCurrentThreadId();

        // close wal raf
        if (raf != null) {
            try {
                raf.close();
                System.out.println("Close wal raf success, slot: " + slot);

                rafShortValue.close();
                System.out.println("Close wal short value raf success, slot: " + slot);
            } catch (IOException e) {
                System.err.println("Close wal raf / wal short raf error, slot: " + slot);
            }
        }

        if (metaChunkSegmentFlagSeq != null) {
            metaChunkSegmentFlagSeq.cleanUp();
        }

        if (metaChunkSegmentIndex != null) {
            metaChunkSegmentIndex.cleanUp();
        }

        if (chunk != null) {
            chunk.cleanUp();
        }

        for (var replPair : replPairs) {
            replPair.bye();
            replPair.close();
        }
    }


    record BeforePersistWalExtFromMerge(ArrayList<Integer> segmentIndexList,
                                        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList) {
        boolean isEmpty() {
            return segmentIndexList.isEmpty();
        }
    }

    record BeforePersistWalExt2FromMerge(ArrayList<Integer> segmentIndexList,
                                         ArrayList<Wal.V> vList) {
    }

    // for performance, before persist wal, read some segment in same wal group and  merge immediately
    BeforePersistWalExtFromMerge readSomeSegmentsBeforePersistWal(int walGroupIndex) {
        var currentSegmentIndex = chunk.currentSegmentIndex();
        var needMergeSegmentIndex = chunk.needMergeSegmentIndex(false, currentSegmentIndex);

        // find continuous segments those wal group index is same from need merge segment index
        // * 4 make sure to find one
        int nextSegmentCount = Math.min(Math.max(walGroupNumber * 4, (chunk.maxSegmentIndex + 1) / 4), 16384);
        final int[] firstSegmentIndexWithReadSegmentCountArray = metaChunkSegmentFlagSeq.iterateAndFindThoseNeedToMerge(
                needMergeSegmentIndex, nextSegmentCount, walGroupIndex, chunk);

        logMergeCount++;
        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;

        // always consider first 10 and last 10 segments
        if (firstSegmentIndexWithReadSegmentCountArray[0] == NO_NEED_MERGE_SEGMENT_INDEX) {
            final int[] arrayLastN = metaChunkSegmentFlagSeq.iterateAndFindThoseNeedToMerge(
                    chunk.maxSegmentIndex - ONCE_PREPARE_SEGMENT_COUNT, ONCE_PREPARE_SEGMENT_COUNT, walGroupIndex, chunk);
            if (arrayLastN[0] != NO_NEED_MERGE_SEGMENT_INDEX) {
                firstSegmentIndexWithReadSegmentCountArray[0] = arrayLastN[0];
                firstSegmentIndexWithReadSegmentCountArray[1] = arrayLastN[1];
            } else {
                final int[] arrayFirstN = metaChunkSegmentFlagSeq.iterateAndFindThoseNeedToMerge(
                        0, ONCE_PREPARE_SEGMENT_COUNT, walGroupIndex, chunk);
                if (arrayFirstN[0] != NO_NEED_MERGE_SEGMENT_INDEX) {
                    firstSegmentIndexWithReadSegmentCountArray[0] = arrayFirstN[0];
                    firstSegmentIndexWithReadSegmentCountArray[1] = arrayFirstN[1];
                }
            }
        }

        if (firstSegmentIndexWithReadSegmentCountArray[0] == NO_NEED_MERGE_SEGMENT_INDEX) {
            if (doLog) {
                log.warn("No segment need merge when persist wal, s={}, i={}", slot, currentSegmentIndex);
            }
            return null;
        }

        var firstSegmentIndex = firstSegmentIndexWithReadSegmentCountArray[0];
        var segmentCount = firstSegmentIndexWithReadSegmentCountArray[1];
        var segmentBytesBatchRead = preadForMerge(firstSegmentIndex, segmentCount);

        ArrayList<Integer> segmentIndexList = new ArrayList<>();
        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE * 10);

        for (int i = 0; i < segmentCount; i++) {
            var segmentIndex = firstSegmentIndex + i;
            int relativeOffsetInBatchBytes = i * chunkSegmentLength;

            // refer to Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // last segments not write at all, need skip
            if (segmentBytesBatchRead == null || relativeOffsetInBatchBytes >= segmentBytesBatchRead.length) {
                setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted, 0L, 0);
                if (doLog) {
                    log.info("Set segment flag to persisted as not write at all, s={}, i={}", slot, segmentIndex);
                }
                continue;
            }

            ChunkMergeJob.readToCvList(cvList, segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength, segmentIndex, this);
            segmentIndexList.add(segmentIndex);
        }

        return new BeforePersistWalExtFromMerge(segmentIndexList, cvList);
    }

    long logMergeCount = 0;

    @SlaveNeedReplay
    void persistWal(boolean isShortValue, Wal targetWal) {
        var walGroupIndex = targetWal.groupIndex;
        var xForBinlog = new XOneWalGroupPersist(isShortValue, true, walGroupIndex);
        if (isShortValue) {
            keyLoader.persistShortValueListBatchInOneWalGroup(walGroupIndex, targetWal.delayToKeyBucketShortValues.values(), xForBinlog);
            return;
        }

        var delayToKeyBucketValues = targetWal.delayToKeyBucketValues;
        var list = new ArrayList<>(delayToKeyBucketValues.values());
        // sort by bucket index for future merge better
        list.sort(Comparator.comparingInt(Wal.V::bucketIndex));

        var ext = readSomeSegmentsBeforePersistWal(walGroupIndex);
        var ext2 = chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex);

        // remove those wal exist
        if (ext != null) {
            var cvList = ext.cvList;
            cvList.removeIf(one -> delayToKeyBucketValues.containsKey(one.key));
            if (!cvList.isEmpty()) {
                for (var one : cvList) {
                    var cv = one.cv;
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash(), keyLoader.bucketsPerSlot);
                    var extWalGroupIndex = Wal.calWalGroupIndex(bucketIndex);
                    if (extWalGroupIndex != walGroupIndex) {
                        throw new IllegalStateException("Wal group index not match, s=" + slot + ", wal group index=" + walGroupIndex + ", ext wal group index=" + extWalGroupIndex);
                    }
                    list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                            one.key, cv.encode(), true));
                }
            }
        }

        if (ext2 != null) {
            var vList = ext2.vList;
            vList.removeIf(one -> delayToKeyBucketValues.containsKey(one.key()));
            if (!vList.isEmpty()) {
                list.addAll(vList);
            }
        }

        if (list.size() > 1000 * 4) {
            log.warn("Ready to persist wal with merged valid cv list, too large, s={}, wal group index={}, list size={}",
                    slot, walGroupIndex, list.size());
        }

        var needMergeSegmentIndexList = chunk.persist(walGroupIndex, list, false, xForBinlog);
        if (needMergeSegmentIndexList == null) {
            throw new IllegalStateException("Persist error, need merge segment index list is null, slot: " + slot);
        }

        if (ext != null && !ext.isEmpty()) {
            var segmentIndexList = ext.segmentIndexList;
            // continuous segment index
            if (segmentIndexList.getLast() - segmentIndexList.getFirst() == segmentIndexList.size() - 1) {
                List<Long> seq0List = new ArrayList<>(segmentIndexList.size());
                for (var ignored : segmentIndexList) {
                    seq0List.add(0L);
                }
                setSegmentMergeFlagBatch(segmentIndexList.getFirst(), segmentIndexList.size(), Flag.merged_and_persisted, seq0List, walGroupIndex);

                for (var segmentIndex : segmentIndexList) {
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted, 0L);
                }
            } else {
                // when read some segments before persist wal, meta is continuous, but segments read from chunk may be null, skip some, so is not continuous anymore
                // usually not happen
                for (var segmentIndex : segmentIndexList) {
                    setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted, 0L, walGroupIndex);
                    xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted, 0L);
                }
            }

            // do not remove, keep segment index continuous, chunk merge job will skip as flag is merged and persisted
//                needMergeSegmentIndexList.removeIf(segmentIndexList::contains);
        }

        if (ext2 != null) {
            var segmentIndexList = ext2.segmentIndexList;
            // usually not continuous
            for (var segmentIndex : segmentIndexList) {
                setSegmentMergeFlag(segmentIndex, Flag.merged_and_persisted, 0L, walGroupIndex);
                xForBinlog.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Flag.merged_and_persisted, 0L);
            }

            chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal(segmentIndexList, walGroupIndex);
        }

        appendBinlog(xForBinlog);

        if (!needMergeSegmentIndexList.isEmpty()) {
            doMergeJob(needMergeSegmentIndexList);
            checkFirstMergedButNotPersistedSegmentIndexTooNear();
        }

        checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(false);
    }

    void checkFirstMergedButNotPersistedSegmentIndexTooNear() {
        if (chunkMergeWorker.isMergedSegmentSetEmpty()) {
            return;
        }

        var currentSegmentIndex = chunk.currentSegmentIndex();
        var firstMergedButNotPersisted = chunkMergeWorker.firstMergedSegmentIndex();

        // need persist merged segments immediately, or next time wal persist will not prepare ready
        boolean needPersistMergedButNotPersisted = isNeedPersistMergedButNotPersisted(currentSegmentIndex, firstMergedButNotPersisted);
        if (needPersistMergedButNotPersisted) {
            log.warn("Persist merged segments immediately, s={}, begin merged segment index={}",
                    slot, firstMergedButNotPersisted);
            chunkMergeWorker.persistFIFOMergedCvList();
        }
    }

    private boolean isNeedPersistMergedButNotPersisted(int currentSegmentIndex, int firstMergedButNotPersisted) {
        boolean needPersistMergedButNotPersisted = false;
        if (currentSegmentIndex < chunk.halfSegmentNumber) {
            if (firstMergedButNotPersisted - currentSegmentIndex <= ONCE_PREPARE_SEGMENT_COUNT) {
                needPersistMergedButNotPersisted = true;
            }
        } else {
            if (firstMergedButNotPersisted > currentSegmentIndex &&
                    firstMergedButNotPersisted - currentSegmentIndex <= ONCE_PREPARE_SEGMENT_COUNT) {
                needPersistMergedButNotPersisted = true;
            }

            // recycle
            if (firstMergedButNotPersisted < currentSegmentIndex &&
                    chunk.maxSegmentIndex - currentSegmentIndex <= ONCE_PREPARE_SEGMENT_COUNT &&
                    firstMergedButNotPersisted <= ONCE_PREPARE_SEGMENT_COUNT) {
                needPersistMergedButNotPersisted = true;
            }
        }
        return needPersistMergedButNotPersisted;
    }

    void checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(boolean isServerStart) {
        var currentSegmentIndex = chunk.currentSegmentIndex();

        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();
        // * 2 when recycled, from 0 again
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT * 2 + chunkMergeWorker.MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST; i++) {
            var targetSegmentIndex = currentSegmentIndex + i;
            if (targetSegmentIndex == chunk.maxSegmentIndex + 1) {
                targetSegmentIndex = 0;
            } else if (targetSegmentIndex > chunk.maxSegmentIndex + 1) {
                // recycle
                targetSegmentIndex = targetSegmentIndex - chunk.maxSegmentIndex - 1;
            }

            var segmentFlag = getSegmentMergeFlag(targetSegmentIndex);
            var flag = segmentFlag.flag();

            if (isServerStart && flag == Flag.reuse) {
                continue;
            }

            if (flag != Flag.init && flag != Flag.merged_and_persisted) {
                needMergeSegmentIndexList.add(targetSegmentIndex);
            }
        }

        if (needMergeSegmentIndexList.isEmpty()) {
            return;
        }

        log.warn("Not merged and persisted next range segment index too near, s={}, begin segment index={}",
                slot, needMergeSegmentIndexList.getFirst());

        needMergeSegmentIndexList.sort(Integer::compareTo);
        // maybe not continuous
        var validCvCountTotal = mergeTargetSegments(needMergeSegmentIndexList, isServerStart);

        if (isServerStart && validCvCountTotal > 0) {
            chunkMergeWorker.persistAllMergedCvListInTargetSegmentIndexList(needMergeSegmentIndexList);
        }
    }

    private void checkSegmentIndex(int segmentIndex) {
        if (segmentIndex < 0 || segmentIndex > chunk.maxSegmentIndex) {
            throw new IllegalStateException("Segment index out of bound, s=" + slot + ", i=" + segmentIndex);
        }
    }

    private void checkBeginSegmentIndex(int beginSegmentIndex, int segmentCount) {
        if (beginSegmentIndex < 0 || beginSegmentIndex + segmentCount > chunk.maxSegmentIndex) {
            throw new IllegalStateException("Begin segment index out of bound, s=" + slot + ", i=" + beginSegmentIndex + ", c=" + segmentCount);
        }
    }

    SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        checkSegmentIndex(segmentIndex);
        return metaChunkSegmentFlagSeq.getSegmentMergeFlag(segmentIndex);
    }

    ArrayList<SegmentFlag> getSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount) {
        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        return metaChunkSegmentFlagSeq.getSegmentMergeFlagBatch(beginSegmentIndex, segmentCount);
    }

    public List<Long> getSegmentSeqListBatchForRepl(int beginSegmentIndex, int segmentCount) {
        checkCurrentThreadId();

        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        return metaChunkSegmentFlagSeq.getSegmentSeqListBatchForRepl(beginSegmentIndex, segmentCount);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void updateSegmentMergeFlag(int segmentIndex, Flag flag, long segmentSeq) {
        var segmentFlag = getSegmentMergeFlag(segmentIndex);
        setSegmentMergeFlag(segmentIndex, flag, segmentSeq, segmentFlag.walGroupIndex());
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void setSegmentMergeFlag(int segmentIndex, Flag flag, long segmentSeq, int walGroupIndex) {
        checkSegmentIndex(segmentIndex);
        metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex, flag, segmentSeq, walGroupIndex);
    }

    void setSegmentMergeFlagBatch(int beginSegmentIndex, int segmentCount, Flag flag, List<Long> segmentSeqList, int walGroupIndex) {
        checkBeginSegmentIndex(beginSegmentIndex, segmentCount);
        metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(beginSegmentIndex, segmentCount, flag, segmentSeqList, walGroupIndex);
    }

    int doMergeJob(ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake);
        return job.run();
    }

    int doMergeJobWhenServerStart(ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake);
        return job.run();
    }

    void persistMergingOrMergedSegmentsButNotPersisted() {
        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();

        this.metaChunkSegmentFlagSeq.iterateAll((segmentIndex, flag, segmentSeq, walGroupIndex) -> {
            if (flag.isMergingOrMerged()) {
                log.warn("Segment not persisted after merging, s={}, i={}, flag={}", slot, segmentIndex, flag);
                needMergeSegmentIndexList.add(segmentIndex);
            }
        });

        if (needMergeSegmentIndexList.isEmpty()) {
            log.warn("No segment need merge when server start, s={}", slot);
        } else {
            mergeTargetSegments(needMergeSegmentIndexList, true);
        }
    }

    private int mergeTargetSegments(ArrayList<Integer> needMergeSegmentIndexList, boolean isServerStart) {
        int validCvCountTotal = 0;

        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();

        // continuous
        if (lastSegmentIndex - firstSegmentIndex + 1 == needMergeSegmentIndexList.size()) {
            var validCvCount = isServerStart ? doMergeJobWhenServerStart(needMergeSegmentIndexList) : doMergeJob(needMergeSegmentIndexList);
            log.warn("Merge segments, is server start: {}, s={}, i={}, end i={}, valid cv count after run: {}",
                    isServerStart, slot, firstSegmentIndex, lastSegmentIndex, validCvCount);
            validCvCountTotal += validCvCount;
        } else {
            // not continuous, need split
            ArrayList<Integer> onceList = new ArrayList<>();
            onceList.add(firstSegmentIndex);

            int last = firstSegmentIndex;
            for (int i = 1; i < needMergeSegmentIndexList.size(); i++) {
                var segmentIndex = needMergeSegmentIndexList.get(i);
                if (segmentIndex - last != 1) {
                    if (!onceList.isEmpty()) {
                        var validCvCount = isServerStart ? doMergeJobWhenServerStart(onceList) : doMergeJob(onceList);
                        log.warn("Merge segments, is server start: {}, once list, s={}, i={}, end i={}, valid cv count after run: {}",
                                isServerStart, slot, onceList.getFirst(), onceList.getLast(), validCvCount);
                        validCvCountTotal += validCvCount;
                        onceList.clear();
                    }
                }
                onceList.add(segmentIndex);
                last = segmentIndex;
            }

            if (!onceList.isEmpty()) {
                var validCvCount = isServerStart ? doMergeJobWhenServerStart(onceList) : doMergeJob(onceList);
                log.warn("Merge segments, is server start: {}, once list, s={}, i={}, end i={}, valid cv count after run: {}",
                        isServerStart, slot, onceList.getFirst(), onceList.getLast(), validCvCount);
                validCvCountTotal += validCvCount;
            }
        }

        return validCvCountTotal;
    }

    void getMergedSegmentIndexEndLastTime() {
        chunk.mergedSegmentIndexEndLastTime = metaChunkSegmentFlagSeq.getMergedSegmentIndexEndLastTime(
                chunk.currentSegmentIndex(), chunk.halfSegmentNumber);
        chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart();
        log.info("Get merged segment index end last time, s={}, i={}", slot, chunk.mergedSegmentIndexEndLastTime);

        chunkMergeWorker.lastMergedSegmentIndex = chunk.mergedSegmentIndexEndLastTime;
    }

    // metrics
    final static SimpleGauge walDelaySizeGauge = new SimpleGauge("wal_delay_size", "wal delay size",
            "slot", "group_index");

    final static SimpleGauge slotInnerGauge = new SimpleGauge("slot_inner", "slot inner",
            "slot");

    static {
        walDelaySizeGauge.register();
        slotInnerGauge.register();
    }

    long segmentDecompressTimeTotalUs = 0;
    long segmentDecompressCountTotal = 0;

    private void initMetricsCollect() {
        walDelaySizeGauge.addRawGetter(() -> {
            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            for (var wal : walArray) {
                var labelValues = List.of(slotStr, String.valueOf(wal.groupIndex));
                map.put("delay_values_size", new SimpleGauge.ValueWithLabelValues((double) wal.delayToKeyBucketValues.size(), labelValues));
                map.put("delay_short_values_size", new SimpleGauge.ValueWithLabelValues((double) wal.delayToKeyBucketShortValues.size(), labelValues));
            }
            return map;
        });

        slotInnerGauge.addRawGetter(() -> {
            var labelValues = List.of(slotStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("dict_size", new SimpleGauge.ValueWithLabelValues((double) DictMap.getInstance().dictSize(), labelValues));
            map.put("last_seq", new SimpleGauge.ValueWithLabelValues((double) snowFlake.getLastNextId(), labelValues));
            map.put("wal_key_count", new SimpleGauge.ValueWithLabelValues((double) getWalKeyCount(), labelValues));

            if (chunk != null) {
                map.put("chunk_current_segment_index", new SimpleGauge.ValueWithLabelValues((double) chunk.currentSegmentIndex(), labelValues));
                map.put("chunk_max_segment_index", new SimpleGauge.ValueWithLabelValues((double) chunk.maxSegmentIndex, labelValues));
            }

            var firstWalGroup = walArray[0];
            map.put("first_wal_group_delay_values_size", new SimpleGauge.ValueWithLabelValues((double) firstWalGroup.delayToKeyBucketValues.size(), labelValues));
            map.put("first_wal_group_delay_short_values_size", new SimpleGauge.ValueWithLabelValues((double) firstWalGroup.delayToKeyBucketShortValues.size(), labelValues));
            map.put("first_wal_group_need_persist_count_total", new SimpleGauge.ValueWithLabelValues((double) firstWalGroup.needPersistCountTotal, labelValues));
            map.put("first_wal_group_need_persist_kv_count_total", new SimpleGauge.ValueWithLabelValues((double) firstWalGroup.needPersistKvCountTotal, labelValues));
            map.put("first_wal_group_need_persist_offset_total", new SimpleGauge.ValueWithLabelValues((double) firstWalGroup.needPersistOffsetTotal, labelValues));

            if (slot == 0) {
                map.put("estimate_key_number", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.estimateKeyNumber, labelValues));
                map.put("estimate_one_value_length", new SimpleGauge.ValueWithLabelValues((double) ConfForSlot.global.estimateOneValueLength, labelValues));

                map.put("lru_prepare_mb_fd_key_bucket_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.fd_key_bucket), labelValues));
                map.put("lru_prepare_mb_fd_chunk_data_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.fd_chunk_data), labelValues));
                map.put("lru_prepare_mb_kv_read_group_by_wal_group_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.kv_read_group_by_wal_group), labelValues));
                map.put("lru_prepare_mb_kv_write_in_wal_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.kv_write_in_wal), labelValues));
                map.put("lru_prepare_mb_kv_big_string_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.big_string), labelValues));
                map.put("lru_prepare_mb_chunk_segment_merged_cv_buffer_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.chunk_segment_merged_cv_buffer), labelValues));

                map.put("lru_prepare_mb_all", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(), labelValues));

                map.put("static_memory_prepare_mb_wal_cache_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.wal_cache), labelValues));
                map.put("static_memory_prepare_mb_wal_cache_init_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.wal_cache_init), labelValues));
                map.put("static_memory_prepare_mb_meta_chunk_segment_flag_seq_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.meta_chunk_segment_flag_seq), labelValues));
                map.put("static_memory_prepare_mb_fd_read_write_buffer_all_slots", new SimpleGauge.ValueWithLabelValues(
                        (double) StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.fd_read_write_buffer), labelValues));
            }

            var hitMissTotal = kvLRUHitTotal + kvLRUMissTotal;
            if (hitMissTotal > 0) {
                map.put("kv_lru_hit_total", new SimpleGauge.ValueWithLabelValues((double) kvLRUHitTotal, labelValues));
                map.put("kv_lru_miss_total", new SimpleGauge.ValueWithLabelValues((double) kvLRUMissTotal, labelValues));
                map.put("kv_lru_hit_rate", new SimpleGauge.ValueWithLabelValues((double) kvLRUHitTotal / hitMissTotal, labelValues));
                map.put("kv_lru_current_count_total", new SimpleGauge.ValueWithLabelValues((double) kvByWalGroupIndexCountTotal(), labelValues));
            }

            if (kvLRUHitTotal > 0) {
                map.put("kv_lru_cv_encoded_length_total", new SimpleGauge.ValueWithLabelValues((double) kvLRUCvEncodedLengthTotal, labelValues));
                var kvLRUCvEncodedLengthAvg = (double) kvLRUCvEncodedLengthTotal / kvLRUHitTotal;
                map.put("kv_lru_cv_encoded_length_avg", new SimpleGauge.ValueWithLabelValues(kvLRUCvEncodedLengthAvg, labelValues));
            }

            if (segmentDecompressCountTotal > 0) {
                map.put("segment_decompress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) segmentDecompressTimeTotalUs, labelValues));
                map.put("segment_decompress_count_total", new SimpleGauge.ValueWithLabelValues((double) segmentDecompressCountTotal, labelValues));
                double segmentDecompressedCostTAvg = (double) segmentDecompressTimeTotalUs / segmentDecompressCountTotal;
                map.put("segment_decompress_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues(segmentDecompressedCostTAvg, labelValues));
            }

            var replPairSize = replPairs.stream().filter(one -> !one.isSendBye()).count();
            map.put("repl_pair_size", new SimpleGauge.ValueWithLabelValues((double) replPairSize, labelValues));
            return map;
        });
    }
}
