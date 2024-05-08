package redis.persist;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.luben.zstd.Zstd;
import io.activej.async.callback.AsyncComputation;
import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import jnr.posix.LibC;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.*;
import redis.command.XGroup;
import redis.repl.MasterUpdateCallback;
import redis.repl.ReplPair;
import redis.repl.SendToSlaveMasterUpdateCallback;
import redis.repl.content.ToMasterExistsSegmentMeta;
import redis.repl.content.ToSlaveWalAppendBatch;
import redis.stats.OfStats;
import redis.stats.StatKV;
import redis.task.ITask;
import redis.task.TaskChain;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static redis.persist.Chunk.SEGMENT_HEADER_LENGTH;

public class OneSlot implements OfStats {
    public OneSlot(byte slot, short slotNumber, SnowFlake snowFlake, File persistDir, Config persistConfig) throws IOException {
        this.segmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.batchNumber = ConfForSlot.global.confWal.batchNumber;

        this.slot = slot;
        this.snowFlake = snowFlake;
        this.persistConfig = persistConfig;

        this.slotDir = new File(persistDir, "slot-" + slot);
        if (!slotDir.exists()) {
            if (!slotDir.mkdirs()) {
                throw new IOException("Create slot dir error, slot: " + slot);
            }
        }

        this.bigStringDir = new File(slotDir, BIG_STRING_DIR_NAME);
        if (!bigStringDir.exists()) {
            if (!bigStringDir.mkdirs()) {
                throw new IOException("Create big string dir error, slot: " + slot);
            }
        }

        var dynConfigFile = new File(slotDir, DYN_CONFIG_FILE_NAME);
        this.dynConfig = new DynConfig(slot, dynConfigFile);

        var masterUuidSaved = dynConfig.getMasterUuid();
        if (masterUuidSaved != null) {
            this.masterUuid = masterUuidSaved;
        } else {
            this.masterUuid = snowFlake.nextId();
            dynConfig.setMasterUuid(masterUuid);
        }

        int bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;
        var walGroupNumber = bucketsPerSlot / ConfForSlot.global.confWal.oneChargeBucketNumber;
        this.walQueueArray = new LinkedBlockingQueue[walGroupNumber];
        this.walsArray = new Wal[walGroupNumber][];
        this.currentWalArray = new Wal[walGroupNumber];

        this.rafArray = new RandomAccessFile[batchNumber];
        this.rafShortValueArray = new RandomAccessFile[batchNumber];

        if (!ConfForSlot.global.pureMemory) {
            for (int i = 0; i < batchNumber; i++) {
                var walSharedFileBatch = new File(slotDir, "wal-" + i + ".dat");
                if (!walSharedFileBatch.exists()) {
                    FileUtils.touch(walSharedFileBatch);

                    var initTimes = walGroupNumber / Wal.INIT_M4_TIMES;
                    for (int j = 0; j < initTimes; j++) {
                        FileUtils.writeByteArrayToFile(walSharedFileBatch, Wal.INIT_M4, true);
                    }
                }
                rafArray[i] = new RandomAccessFile(walSharedFileBatch, "rw");

                var walSharedFileShortValueBatch = new File(slotDir, "wal-short-value-" + i + ".dat");
                if (!walSharedFileShortValueBatch.exists()) {
                    FileUtils.touch(walSharedFileShortValueBatch);

                    var initTimes = walGroupNumber / Wal.INIT_M4_TIMES;
                    for (int j = 0; j < initTimes; j++) {
                        FileUtils.writeByteArrayToFile(walSharedFileShortValueBatch, Wal.INIT_M4, true);
                    }
                }
                rafShortValueArray[i] = new RandomAccessFile(walSharedFileShortValueBatch, "rw");
            }
        }

        for (int i = 0; i < walGroupNumber; i++) {
            walQueueArray[i] = new LinkedBlockingQueue<>(batchNumber);
            walsArray[i] = new Wal[batchNumber];

            for (int j = 0; j < batchNumber; j++) {
                var wal = new Wal(slot, i, (byte) j, rafArray[j], rafShortValueArray[j], snowFlake);
                walsArray[i][j] = wal;
                if (j > 0) {
                    walQueueArray[i].add(wal);
                } else {
                    // first wal as current
                    currentWalArray[i] = wal;
                    wal.lastUsedTimeMillis = System.currentTimeMillis();
                }
            }
        }

        // default 2000, I do not know if it is suitable
        var sendOnceMaxCount = persistConfig.get(ofInteger(), "repl.wal.sendOnceMaxCount", 2000);
        var sendOnceMaxSize = persistConfig.get(ofInteger(), "repl.wal.sendOnceMaxSize", 1024 * 1024);
        var toSlaveWalAppendBatch = new ToSlaveWalAppendBatch(sendOnceMaxCount, sendOnceMaxSize);
        // sync to slave callback
        this.masterUpdateCallback = new SendToSlaveMasterUpdateCallback(() -> {
            // merge worker thread also call this, so need thread safe
            synchronized (replPairs) {
                return replPairs.stream().filter(ReplPair::isAsMaster).collect(Collectors.toList());
            }
        }, toSlaveWalAppendBatch);

        this.keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, masterUpdateCallback, dynConfig);

        DictMap.getInstance().setMasterUpdateCallback(masterUpdateCallback);

        this.persistHandleEventloopArray = new Eventloop[batchNumber];
        final int idleMillis = 10;
        for (int i = 0; i < batchNumber; i++) {
            var persistHandleEventloop = Eventloop.builder()
                    .withThreadName("persist-worker-slot-" + slot + "-batch-" + i)
                    .withIdleInterval(Duration.ofMillis(idleMillis))
                    .build();
            persistHandleEventloop.keepAlive(true);
            this.persistHandleEventloopArray[i] = persistHandleEventloop;

            var thread = chunkMerger.getPersistThreadFactoryForSlot(slot, slotNumber).newThread(persistHandleEventloop);
            thread.start();
            log.info("Slot persist handle eventloop thread started, s={}, b={}", slot, i);
        }

        this.compressStats = new CompressStats("slot-" + slot);

        this.initSegmentCache();
        this.initTasks();
    }

    private void initSegmentCache() {
        var lru = ConfForSlot.global.confChunk.lru;
        // 10MB
        var maximumBytes = ConfForSlot.global.pureMemory ? Math.min(1024 * 1024 * 10L, lru.maximumBytes) : lru.maximumBytes;
        this.readPersistedSegmentCache = Caffeine.newBuilder()
                .recordStats()
                .expireAfterWrite(lru.expireAfterWrite, TimeUnit.SECONDS)
                .expireAfterAccess(lru.expireAfterAccess, TimeUnit.SECONDS)
                .maximumWeight(maximumBytes)
                .weigher((SegmentCacheKey k, byte[] v) -> v.length)
                .build();
        log.info("Read segment cache init, expire after write: {} s, expire after access: {} s, maximum bytes: {}",
                lru.expireAfterWrite, lru.expireAfterAccess, lru.maximumBytes);
    }


    private final Logger log = LoggerFactory.getLogger(OneSlot.class);

    public long getMasterUuid() {
        return masterUuid;
    }

    private final long masterUuid;

    private final ArrayList<ReplPair> replPairs = new ArrayList<>();

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

    private final LinkedList<ReplPair> delayNeedCloseReplPairs = new LinkedList<>();

    public void addDelayNeedCloseReplPair(ReplPair replPair) {
        delayNeedCloseReplPairs.add(replPair);
    }

    // todo, both master - master, need change equal and init as master or slave
    public void createReplPairAsSlave(String host, int port) throws IOException {
        // remove old if exists
        removeReplPairAsSlave();

        var replPair = new ReplPair(slot, false, host, port);
        replPair.setSlaveUuid(masterUuid);
        replPair.initAsSlave(requestHandleEventloop, requestHandler);
        log.warn("Create repl pair as slave, host: {}, port: {}, slot: {}", host, port, slot);
        replPairs.add(replPair);

        if (!isReadonly()) {
            setReadonly(true);
        }
        if (canRead()) {
            setCanRead(false);
        }
    }

    public void removeReplPairAsSlave() throws IOException {
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (!replPair.isAsMaster()) {
                replPair.bye();
                addDelayNeedCloseReplPair(replPair);
                return;
            }
        }

        if (isReadonly()) {
            setReadonly(false);
        }
        if (!canRead()) {
            setCanRead(true);
        }
    }

    public ReplPair getReplPairAsMaster(long slaveUuid) {
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (replPair.isAsMaster() && replPair.getSlaveUuid() == slaveUuid) {
                return replPair;
            }
        }
        return null;
    }

    public ReplPair getReplPairAsSlave(long slaveUuid) {
        for (var replPair : replPairs) {
            if (replPair.isSendBye()) {
                continue;
            }

            if (!replPair.isAsMaster() && replPair.getSlaveUuid() == slaveUuid) {
                return replPair;
            }
        }
        return null;
    }

    public ReplPair createIfNotExistReplPairAsMaster(long slaveUuid, String host, int port) {
        var replPair = new ReplPair(slot, true, host, port);
        replPair.setSlaveUuid(slaveUuid);
        replPair.setMasterUuid(masterUuid);

        for (var replPair1 : replPairs) {
            if (replPair1.equals(replPair)) {
                log.warn("Repl pair already exists, host: {}, port: {}, slot: {}", host, port, slot);
                replPair1.initAsMaster(slaveUuid, requestHandleEventloop, requestHandler);
                return replPair1;
            }
        }

        replPair.initAsMaster(slaveUuid, requestHandleEventloop, requestHandler);
        log.warn("Create repl pair as master, host: {}, port: {}, slot: {}", host, port, slot);
        replPairs.add(replPair);
        return replPair;
    }

    public void setRequestHandleEventloop(Eventloop requestHandleEventloop) {
        this.requestHandleEventloop = requestHandleEventloop;
    }

    private Eventloop requestHandleEventloop;

    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private RequestHandler requestHandler;

    public <T> CompletableFuture<T> threadSafeHandle(SupplierEx<T> supplierEx) {
        if (reuseNetWorkers) {
            try {
                var r = supplierEx.get();
                return CompletableFuture.completedFuture(r);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        return requestHandleEventloop.submit(AsyncComputation.of(supplierEx));
    }

    private final byte slot;

    public byte slot() {
        return slot;
    }

    private final int segmentLength;
    private final int batchNumber;
    private final SnowFlake snowFlake;
    private final Config persistConfig;
    private final File slotDir;

    private static final String BIG_STRING_DIR_NAME = "big-string";
    private final File bigStringDir;

    public List<Long> getBigStringFileUuidList() {
        var list = new ArrayList<Long>();
        File[] files = bigStringDir.listFiles();
        for (File file : files) {
            list.add(Long.parseLong(file.getName()));
        }
        return list;
    }

    private static final String DYN_CONFIG_FILE_NAME = "dyn-config.json";
    private final DynConfig dynConfig;

    private static final ArrayList<String> dynConfigKeyWhiteList = new ArrayList<>();

    static {
        // add white list here
        dynConfigKeyWhiteList.add("testKey");
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

    public boolean canRead() {
        return dynConfig.isCanRead();
    }

    public void setCanRead(boolean canRead) throws IOException {
        dynConfig.setCanRead(canRead);
    }

    public File getBigStringDir() {
        return bigStringDir;
    }

    // index is group index
    private final LinkedBlockingQueue<Wal>[] walQueueArray;
    private long takeWalCostNanos = 0;
    private long takeWalCount = 0;

    // first index is group index, second index is batch index
    private final Wal[][] walsArray;
    // index is group index
    private final Wal[] currentWalArray;

    // index is batch index
    private final RandomAccessFile[] rafArray;
    private final RandomAccessFile[] rafShortValueArray;

    final KeyLoader keyLoader;

    public KeyLoader getKeyLoader() {
        return keyLoader;
    }

    private final MasterUpdateCallback masterUpdateCallback;

    public long getKeyCount() {
        var r = keyLoader.getKeyCount();
        for (var wals : walsArray) {
            for (var wal : wals) {
                r += wal.getKeyCount();
            }
        }
        return r;
    }

    public short getKeyCountInBucketIndex(int bucketIndex) {
        return keyLoader.getKeyCountInBucketIndex(bucketIndex);
    }

    public ArrayList<KeyBucket> getKeyBuckets(int bucketIndex) {
        return keyLoader.getKeyBuckets(bucketIndex);
    }

    private final Eventloop[] persistHandleEventloopArray;

    private LibC libC;

    public byte getAllWorkers() {
        return allWorkers;
    }

    private byte allWorkers;
    private byte requestWorkers;
    private byte mergeWorkers;
    private byte topMergeWorkers;

    private ChunkMerger chunkMerger;

    public ChunkMerger getChunkMerger() {
        return chunkMerger;
    }

    public void setChunkMerger(ChunkMerger chunkMerger) {
        this.chunkMerger = chunkMerger;

        for (int i = requestWorkers; i < allWorkers; i++) {
            for (var chunk : chunksArray[i]) {
                chunkMerger.getChunkMergeWorker((byte) i).fixMergeHandleChunkThreadId(chunk);
            }
        }

        chunkMerger.putMasterUpdateCallback(slot, masterUpdateCallback);
    }

    // first index is worker id, second index is batch index
    Chunk[][] chunksArray;

    private MetaChunkSegmentFlagSeq metaChunkSegmentFlagSeq;

    public byte[] getMetaChunkSegmentFlagSeqBytesOneWorkerOneBatchToSlaveExists(byte workerId, byte batchIndex) {
        return metaChunkSegmentFlagSeq.getInMemoryCachedBytesOneWorkerOneBatch(workerId, batchIndex);
    }

    public void overwriteMetaChunkSegmentFlagSeqBytesOneWorkerFromMasterExists(byte[] bytes) {
        metaChunkSegmentFlagSeq.overwriteInMemoryCachedBytesOneWorker(bytes);
    }

    private MetaChunkSegmentIndex metaChunkSegmentIndex;

    // read only, important
    public byte[] getMetaChunkSegmentIndexBytesToSlaveExists() {
        return metaChunkSegmentIndex.getInMemoryCachedBytes();
    }

    public void overwriteMetaChunkSegmentIndexBytesFromRepl(byte[] bytes) {
        metaChunkSegmentIndex.overwriteInMemoryCachedBytes(bytes);
    }

    private final CompressStats compressStats;

    boolean reuseNetWorkers;

    private final TaskChain taskChain = new TaskChain();

    public void doTask(int loopCount) {
        for (var t : taskChain.list) {
            if (loopCount % t.executeOnceAfterLoopCount() == 0) {
                t.setLoopCount(loopCount);

                try {
                    t.run();
                } catch (Exception e) {
                    log.error("Task error, name: " + t.name(), e);
                }
            }
        }
    }

    private void initTasks() {
        taskChain.add(new ITask() {
            private int loopCount = 0;

            @Override
            public String name() {
                return "repl pair client ping/server flush wal append batch";
            }

            @Override
            public void run() {
                for (var replPair : replPairs) {
                    if (replPair.isSendBye()) {
                        continue;
                    }

                    if (!replPair.isAsMaster()) {
                        // only slave need send ping
                        replPair.ping();
                    } else {
                        if (!masterUpdateCallback.isToSlaveWalAppendBatchEmpty()) {
                            masterUpdateCallback.flushToSlaveWalAppendBatch();
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
                            log.warn("Remove repl pair after bye, host: {}, port: {}, slot: {}", replPair.getHost(), replPair.getPort(), slot);
                            break;
                        }
                    }
                }
            }

            @Override
            public void setLoopCount(int loopCount) {
                this.loopCount = loopCount;
            }

            @Override
            public int executeOnceAfterLoopCount() {
                return 1;
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

    @Override
    public List<StatKV> stats() {
        ArrayList<StatKV> list = new ArrayList<>();

        list.add(StatKV.split);
        list.addAll(keyLoader.stats());
        list.add(StatKV.split);

        var stats = readPersistedSegmentCache.stats();
        final String prefix = "segment cache ";
        OfStats.cacheStatsToList(list, stats, prefix);
        list.add(StatKV.split);

        list.addAll(compressStats.stats());
        list.add(StatKV.split);

        var firstChunk = chunksArray[0][0];
        list.addAll(firstChunk.stats());
        list.add(StatKV.split);

        list.add(new StatKV("wal-s-" + slot + "-group-0 queue size", walQueueArray[0].size()));
        list.add(new StatKV("wal-s-" + slot + " take wal cost nanos", takeWalCostNanos));
        list.add(new StatKV("wal-s-" + slot + " take wal count", takeWalCount));
        if (takeWalCount > 0) {
            list.add(new StatKV("wal-s-" + slot + " take wal cost avg nanos", (double) takeWalCostNanos / takeWalCount));
        }
        list.add(StatKV.split);

        if (!replPairs.isEmpty()) {
            list.add(StatKV.split);
            for (var replPair : replPairs) {
                if (replPair.isSendBye()) {
                    continue;
                }

                list.add(new StatKV("repl pair link up to " + replPair.getHost() + ":" + replPair.getPort(), replPair.isLinkUp() ? 1 : 0));
            }
            list.add(StatKV.split);
        }

        return list;
    }

    private record SegmentCacheKey(byte workerId, byte batchIndex, int segmentIndex, long uuid) {
        static final byte BIG_STRING_BATCH_INDEX = -1;

        // need hashcode ?
        @Override
        public String toString() {
            return "SegmentCacheKey{" +
                    "workerId=" + workerId +
                    ", batchIndex=" + batchIndex +
                    ", segmentIndex=" + segmentIndex +
                    ", uuid=" + uuid +
                    '}';
        }
    }

    private final Function<SegmentCacheKey, @PolyNull byte[]> fnLoadPersistedSegmentBytes = (segmentCacheKey) -> {
        if (segmentCacheKey.batchIndex >= 0) {
            return preadSegmentTightBytesWithLength(segmentCacheKey.workerId, segmentCacheKey.batchIndex, segmentCacheKey.segmentIndex);
        }

        if (segmentCacheKey.batchIndex == SegmentCacheKey.BIG_STRING_BATCH_INDEX) {
            var file = new File(OneSlot.this.bigStringDir, String.valueOf(segmentCacheKey.uuid));
            if (!file.exists()) {
                log.warn("Big string file not exists, uuid: {}", segmentCacheKey.uuid);
                return null;
            }

            try {
                return FileUtils.readFileToByteArray(file);
            } catch (IOException e) {
                log.error("Read big string file error, uuid: " + segmentCacheKey.uuid, e);
                return null;
            }
        }

        return null;
    };

    private Cache<SegmentCacheKey, byte[]> readPersistedSegmentCache;

    void refreshReadPersistedSegmentCache(byte workerId, byte batchIndex, int segmentIndex, byte[] tightBytesWithLength) {
        var k = new SegmentCacheKey(workerId, batchIndex, segmentIndex, 0);
        readPersistedSegmentCache.put(k, tightBytesWithLength);
    }

    public ByteBuf get(byte[] keyBytes, int bucketIndex, long keyHash) {
        var key = new String(keyBytes);
        var tmpValueBytes = getFromWal(key, bucketIndex);
        if (tmpValueBytes != null) {
            // write batch kv is the newest
            if (CompressedValue.isDeleted(tmpValueBytes)) {
                return null;
            }
            return Unpooled.wrappedBuffer(tmpValueBytes);
        }

        var valueBytesWithExpireAt = keyLoader.get(bucketIndex, keyBytes, keyHash);
        if (valueBytesWithExpireAt == null) {
            return null;
        }

        // if value bytes is not meta, must be short value
        var valueBytes = valueBytesWithExpireAt.valueBytes();
        if (!PersistValueMeta.isPvm(valueBytes)) {
            return Unpooled.wrappedBuffer(valueBytes);
        }

        var pvm = PersistValueMeta.decode(valueBytes);

        // need not lock, write is always append only, old value will not be changed
        var withKeyHeaderBuf = getKeyValueBufByPvm(pvm);

        // skip key header
        // no use, for check
        byte keyLength = withKeyHeaderBuf.readByte();
        var keyBytesRead = new byte[keyLength];
        withKeyHeaderBuf.readBytes(keyBytesRead);

//        if (!Arrays.equals(keyBytesRead, keyBytes)) {
//            throw new IllegalStateException("Key not match, key: " + new String(keyBytes) + ", key persisted: " + new String(keyBytesRead));
//        }

        return withKeyHeaderBuf;
    }

    byte[] getFromWal(String key, int bucketIndex) {
        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
        byte[] tmpValueBytes = currentWalArray[walGroupIndex].get(key);
        long lastUsedTimeMillis = currentWalArray[walGroupIndex].lastUsedTimeMillis;
        for (var wal : walsArray[walGroupIndex]) {
            if (wal == currentWalArray[walGroupIndex]) {
                continue;
            }

            // skip older
            if (lastUsedTimeMillis > wal.lastUsedTimeMillis && tmpValueBytes != null) {
                continue;
            }

            lastUsedTimeMillis = wal.lastUsedTimeMillis;

            var tmpValueBytesThisWal = wal.get(key);
            if (tmpValueBytesThisWal != null) {
                tmpValueBytes = tmpValueBytesThisWal;
            }
        }
        return tmpValueBytes;
    }

    public byte[] getBigStringFromCache(long uuid) {
        var segmentCacheKey = new SegmentCacheKey((byte) 0, SegmentCacheKey.BIG_STRING_BATCH_INDEX, 0, uuid);
        return readPersistedSegmentCache.get(segmentCacheKey, fnLoadPersistedSegmentBytes);
    }

    public ByteBuf getKeyValueBufByPvm(PersistValueMeta pvm) {
        var perfTestReadSegmentNoCache = Debug.getInstance().perfTestReadSegmentNoCache;

        // load from segment lru cache
        // one key value pair only store in one segment
        byte[] tightBytesWithLength;
        if (!perfTestReadSegmentNoCache) {
            if (ConfForSlot.global.pureMemory) {
                tightBytesWithLength = preadSegmentTightBytesWithLength(pvm.workerId, pvm.batchIndex, pvm.segmentIndex);
            } else {
                var segmentCacheKey = new SegmentCacheKey(pvm.workerId, pvm.batchIndex, pvm.segmentIndex, 0);
                tightBytesWithLength = readPersistedSegmentCache.get(segmentCacheKey, fnLoadPersistedSegmentBytes);
            }
        } else {
            // ignore big string, just for no cache, ssd read performance test
            tightBytesWithLength = preadSegmentTightBytesWithLength(pvm.workerId, pvm.batchIndex, pvm.segmentIndex);
        }
        if (tightBytesWithLength == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm: " + pvm);
        }

        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        // refer to SegmentBatch tight HEADER_LENGTH
        buffer.position(4 + pvm.subBlockIndex * 4);
        var subBlockOffset = buffer.getShort();
        var subBlockLength = buffer.getShort();

        var uncompressedBytes = new byte[segmentLength];
        long begin = System.nanoTime();
        var d = Zstd.decompressByteArray(uncompressedBytes, 0, segmentLength,
                tightBytesWithLength, subBlockOffset, subBlockLength);
        long costT = System.nanoTime() - begin;
        if (d != segmentLength) {
            throw new IllegalStateException("Decompress error, w=" + pvm.workerId + ", s=" + pvm.slot +
                    ", b=" + pvm.batchIndex + ", i=" + pvm.segmentIndex + ", sbi=" + pvm.subBlockIndex + ", d=" + d + ", segmentLength=" + segmentLength);
        }

        // thread safe, need not long adder
        compressStats.decompressCount2.increment();
        compressStats.decompressCostTotalTimeNanos2.add(costT);

        var buf = Unpooled.wrappedBuffer(uncompressedBytes);
        buf.readerIndex(pvm.segmentOffset);
        return buf;
    }

    public boolean remove(byte workerId, int bucketIndex, String key, long keyHash) {
        boolean[] isDeletedArr = {false};
        keyLoader.bucketLock(bucketIndex, () -> {
            var isRemovedFromWal = removeFromWal(workerId, bucketIndex, key, keyHash);
            isDeletedArr[0] = isRemovedFromWal || keyLoader.remove(bucketIndex, key.getBytes(), keyHash);
        });
        return isDeletedArr[0];
    }

    public void removeDelay(byte workerId, String key, int bucketIndex, long keyHash) {
        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
        var currentWal = currentWalArray[walGroupIndex];
        var putResult = currentWal.removeDelay(workerId, key, bucketIndex, keyHash);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, bucketIndex, putResult);
        } else {
            if (masterUpdateCallback != null) {
                masterUpdateCallback.onWalAppend(slot, bucketIndex, currentWal.batchIndex,
                        putResult.isValueShort(), putResult.needPutV(), putResult.offset());
            }
        }
    }

    private boolean removeFromWal(byte workerId, int bucketIndex, String key, long keyHash) {
        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;

        boolean isRemoved = false;
        for (var wal : walsArray[walGroupIndex]) {
            var isRemovedThisWal = wal.remove(key);
            if (isRemovedThisWal) {
                isRemoved = true;
            }
        }

        if (isRemoved) {
            removeDelay(workerId, key, bucketIndex, keyHash);
        }
        return isRemoved;
    }

    long threadIdProtectedWhenPut = -1;

    // thread safe, same slot, same event loop
    public void put(byte workerId, String key, int bucketIndex, CompressedValue cv) {
        checkCurrentThread(workerId);
        if (isReadonly()) {
            throw new ReadonlyException();
        }

        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
        var currentWal = currentWalArray[walGroupIndex];

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
        var v = new Wal.V(workerId, cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                key, cvEncoded, cv.compressedLength());

        // for big string, use single file
        boolean isPersistLengthOverSegmentLength = v.persistLength() + SEGMENT_HEADER_LENGTH > segmentLength;
        if (isPersistLengthOverSegmentLength || key.startsWith("kerry-test-big-string-")) {
            var uuid = snowFlake.nextId();
            var bytes = cv.getCompressedData();

            var uuidAsFileName = String.valueOf(uuid);
            var file = new File(bigStringDir, uuidAsFileName);
            try {
                FileUtils.writeByteArrayToFile(file, bytes);
            } catch (IOException e) {
                throw new RuntimeException("Write big string error, key=" + key, e);
            }

            if (masterUpdateCallback != null) {
                masterUpdateCallback.onBigStringFileWrite(slot, uuid, bytes);
            }

            // encode again
            cvEncoded = cv.encodeAsBigStringMeta(uuid);
            v = new Wal.V(workerId, cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cvEncoded, cv.compressedLength());

            isValueShort = true;
        }

        var putResult = currentWal.put(isValueShort, key, v);
        if (!putResult.needPersist()) {
            if (masterUpdateCallback != null) {
                masterUpdateCallback.onWalAppend(slot, bucketIndex, currentWal.batchIndex,
                        isValueShort, v, putResult.offset());
            }

            return;
        }

        doPersist(walGroupIndex, key, bucketIndex, putResult);
    }

    private void checkCurrentThread(byte workerId) {
        var currentThreadId = Thread.currentThread().threadId();
        if (threadIdProtectedWhenPut != -1 && threadIdProtectedWhenPut != currentThreadId) {
            throw new IllegalStateException("Thread id not match, w=" + workerId + ", s=" + slot +
                    ", t=" + currentThreadId + ", t2=" + threadIdProtectedWhenPut);
        }
    }

    private void doPersist(int walGroupIndex, String key, int bucketIndex, Wal.PutResult putResult) {
        var beginT = System.nanoTime();
        try {
            var nextAvailableWal = walQueueArray[walGroupIndex].take();
            var costT = System.nanoTime() - beginT;
            takeWalCostNanos += costT;
            takeWalCount++;

            // clear values in this thread, so RandomAccessFile seek/put is thread safe
            if (putResult.isValueShort()) {
                nextAvailableWal.clearShortValues();
            } else {
                nextAvailableWal.clearValues();
            }

            var needPutV = putResult.needPutV();
            if (needPutV != null) {
                nextAvailableWal.put(putResult.isValueShort(), key, needPutV);

                if (masterUpdateCallback != null) {
                    masterUpdateCallback.onWalAppend(slot, bucketIndex, nextAvailableWal.batchIndex,
                            putResult.isValueShort(), needPutV, putResult.offset());
                }
            }

            submitPersistTaskFromWal(putResult.isValueShort(), walGroupIndex, currentWalArray[walGroupIndex]);
            currentWalArray[walGroupIndex] = nextAvailableWal;
            nextAvailableWal.lastUsedTimeMillis = System.currentTimeMillis();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void asSlaveOnMasterWalAppendBatchGet(TreeMap<Integer, ArrayList<XGroup.ExtV>> extVsGroupByWalGroupIndex) {
        for (var entry : extVsGroupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var extVs = entry.getValue();

            // batch index is not single, can not write once
            for (var extV : extVs) {
                var batchIndex = extV.batchIndex();
                var wal = walsArray[walGroupIndex][batchIndex];

                var v = extV.v();
                var offset = extV.offset();
                if (offset == 0) {
                    // clear
                    if (extV.isValueShort()) {
                        wal.delayToKeyBucketShortValues.clear();
                    } else {
                        wal.delayToKeyBucketValues.clear();
                    }
                }

                wal.writeRafAndOffsetFromMasterNewly(extV.isValueShort(), v, offset);

                var key = v.key();
                if (extV.isValueShort()) {
                    wal.delayToKeyBucketShortValues.put(key, v);
                    wal.delayToKeyBucketValues.remove(key);
                } else {
                    wal.delayToKeyBucketValues.put(key, v);
                    wal.delayToKeyBucketShortValues.remove(key);
                }
            }
        }
    }

    private HashMap<Short, LinkedList<ToMasterExistsSegmentMeta.OncePull>> oncePullsByWorkerAndBatchIndex = new HashMap<>();

    public void resetOncePulls(byte workerId, byte batchIndex, LinkedList<ToMasterExistsSegmentMeta.OncePull> oncePulls) {
        var key = (short) ((workerId << 8) | batchIndex);
        this.oncePullsByWorkerAndBatchIndex.put(key, oncePulls);
    }

    public ToMasterExistsSegmentMeta.OncePull removeOncePull(byte workerId, byte batchIndex, int beginSegmentIndex) {
        var key = (short) ((workerId << 8) | batchIndex);
        var oncePulls = oncePullsByWorkerAndBatchIndex.get(key);
        if (oncePulls == null) {
            return null;
        }

        var it = oncePulls.iterator();
        while (it.hasNext()) {
            var oncePull = it.next();
            if (oncePull.beginSegmentIndex() == beginSegmentIndex) {
                it.remove();
                break;
            }
        }

        var it2 = oncePulls.iterator();
        while (it2.hasNext()) {
            var oncePull = it2.next();
            if (oncePull.beginSegmentIndex() > beginSegmentIndex) {
                return oncePull;
            }
        }

        return null;
    }

    public void flush() {
        // can truncate all batch for better perf, todo
        for (var wals : walsArray) {
            for (var wal : wals) {
                wal.clear();
            }
        }

        this.keyLoader.flush();
        this.metaChunkSegmentFlagSeq.clear();
        this.metaChunkSegmentIndex.clear();
    }

    public void initChunks(LibC libC, byte allWorkers, byte requestWorkers, byte mergeWorkers, byte topMergeWorkers) throws IOException {
        this.allWorkers = allWorkers;
        this.requestWorkers = requestWorkers;
        this.mergeWorkers = mergeWorkers;
        this.topMergeWorkers = topMergeWorkers;

        this.libC = libC;
        this.keyLoader.init(libC, persistConfig);

        // meta data
        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, allWorkers, slotDir);

        // todo, check
//        int[] countArr = new int[batchNumber];
//        this.chunkSegmentFlagMmapBuffer.iterate((workerId, batchIndex, segmentIndex, flag, flagWorkerId) -> {
//            if (flag == Chunk.SEGMENT_FLAG_REUSE) {
//                log.warn("Segment is still reuse wait for merging, w={}, s={}, b={}, i={}, flag={}", workerId, slot, batchIndex, segmentIndex, flag);
//                countArr[batchIndex]++;
//            }
//        });
//
//        for (int batchIndex = 0; batchIndex < batchNumber; batchIndex++) {
//            if (countArr[batchIndex] > 1) {
//                throw new IllegalStateException("More than one segment is still reuse wait for merging, count: " + countArr[batchIndex] +
//                        ", slot: " + slot + ", batch index: " + batchIndex);
//            }
//        }

        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, allWorkers, slotDir);

        // chunks
        this.chunksArray = new Chunk[allWorkers][batchNumber];
        for (int i = 0; i < allWorkers; i++) {
            var chunks = new Chunk[batchNumber];
            chunksArray[i] = chunks;

            var workerId = (byte) i;
            for (int j = 0; j < batchNumber; j++) {
                var chunk = new Chunk(workerId, slot, (byte) j, requestWorkers, snowFlake, slotDir, this, keyLoader, masterUpdateCallback);
                chunks[j] = chunk;

                initChunk(chunk);

                if (i < requestWorkers) {
                    fixRequestHandleChunkThreadId(chunk);
                }
            }
        }
    }

    private void initChunk(Chunk chunk) throws IOException {
        chunk.initFds(libC);

        var segmentIndex = getChunkWriteSegmentIndex(chunk.workerId, chunk.batchIndex);

        // write index mmap crash recovery
        boolean isBreak = false;
        for (int i = 0; i < 10; i++) {
            boolean canWrite = chunk.initIndex(segmentIndex);
            // when restart server, set persisted flag
            if (!canWrite) {
                log.warn("Segment can not write, w={}, s={}, b={}, i={}", chunk.workerId, slot, chunk.batchIndex, chunk.segmentIndex);

                // set persisted flag, for next loop reuse
                setSegmentMergeFlag(chunk.workerId, chunk.batchIndex, chunk.segmentIndex, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, Chunk.MAIN_WORKER_ID, snowFlake.nextId());
                log.warn("Reset persisted when init");

                chunk.moveIndexNext(1);
                setChunkWriteSegmentIndex(chunk.workerId, chunk.batchIndex, chunk.segmentIndex);

                log.warn("Move to next segment, w={}, s={}, b={}, i={}", chunk.workerId, slot, chunk.batchIndex, chunk.segmentIndex);
            } else {
                isBreak = true;
                break;
            }
        }

        if (!isBreak) {
            throw new IllegalStateException("Segment can not write after reset flag, w=" + chunk.workerId +
                    ", s=" + slot + ", b=" + chunk.batchIndex + ", i=" + chunk.segmentIndex);
        }
    }

    private void fixRequestHandleChunkThreadId(Chunk chunk) {
        var persistHandleEventloop = persistHandleEventloopArray[chunk.batchIndex];
        persistHandleEventloop.submit(() -> {
            chunk.threadIdProtectedWhenWrite = Thread.currentThread().threadId();
            chunk.setWorkerType(true, false, false);
            log.warn("Fix request worker chunk chunk thread id, w={}, rw={}, s={}, b={}, tid={}",
                    chunk.workerId, chunk.workerId, slot, chunk.batchIndex, chunk.threadIdProtectedWhenWrite);
        });
    }

    public void submitPersistTaskFromMasterNewly(byte workerId, byte batchIndex, int segmentLength, int segmentIndex, int segmentCount,
                                                 List<Long> segmentSeqList, byte[] bytes, int capacity) {
        var chunk = chunksArray[workerId][batchIndex];
        if (chunk.segmentLength != segmentLength) {
            throw new IllegalStateException("Segment length not match, chunk segment length: " + chunk.segmentLength +
                    ", repl segment length: " + segmentLength);
        }

        if (workerId < requestWorkers) {
            var persistHandleEventloop = persistHandleEventloopArray[batchIndex];
            persistHandleEventloop.submit(() -> {
                chunk.writeSegmentsFromMasterNewly(bytes, segmentIndex, segmentCount, segmentSeqList, capacity);
            });
        } else {
            // merge worker
            chunkMerger.getChunkMergeWorker(workerId).submitWriteSegmentsMasterNewly(chunk,
                    bytes, segmentIndex, segmentCount, segmentSeqList, capacity);
        }
    }

    public void submitPersistTaskFromMasterExists(byte workerId, byte batchIndex, int segmentIndex, int segmentCount,
                                                  List<Long> segmentSeqList, byte[] bytes) {
        var chunk = chunksArray[workerId][batchIndex];
        if (bytes.length != chunk.segmentLength * segmentCount) {
            throw new IllegalStateException("Bytes length not match, bytes length: " + bytes.length +
                    ", segment length: " + chunk.segmentLength + ", segment count: " + segmentCount);
        }

        if (workerId < requestWorkers) {
            var persistHandleEventloop = persistHandleEventloopArray[batchIndex];
            persistHandleEventloop.submit(() -> {
                chunk.writeSegmentsFromMasterNewly(bytes, segmentIndex, segmentCount, segmentSeqList, bytes.length);
            });
        } else {
            // merge worker
            chunkMerger.getChunkMergeWorker(workerId).submitWriteSegmentsMasterNewly(chunk,
                    bytes, segmentIndex, segmentCount, segmentSeqList, bytes.length);
        }
    }

    public int preadForMerge(byte workerId, byte batchIndex, int segmentIndex, ByteBuffer buffer, long offset) {
        var chunk = chunksArray[workerId][batchIndex];
        return chunk.preadForMerge(segmentIndex, buffer, offset);
    }

    public byte[] preadSegmentTightBytesWithLength(byte workerId, byte batchIndex, int segmentIndex) {
        var chunk = chunksArray[workerId][batchIndex];
        return chunk.preadSegmentTightBytesWithLength(segmentIndex);
    }

    public byte[] preadForRepl(byte workerId, byte batchIndex, int segmentIndex) {
        var chunk = chunksArray[workerId][batchIndex];

        var persistHandleEventloop = persistHandleEventloopArray[batchIndex];
        var f = persistHandleEventloop.submit(AsyncComputation.of(() -> chunk.preadForRepl(segmentIndex)));
        try {
            return f.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanUp() {
        // persist handle eventloop break before chunk clean up
        for (var persistHandleEventloop : persistHandleEventloopArray) {
            persistHandleEventloop.breakEventloop();
        }
        System.out.println("Slot persist handle eventloop threads stopped, slot: " + slot);

        // close wal raf
        try {
            for (var raf : rafArray) {
                raf.close();
            }
            System.out.println("Close wal raf success, slot: " + slot);

            for (var raf : rafShortValueArray) {
                raf.close();
            }
            System.out.println("Close wal short value raf success, slot: " + slot);
        } catch (IOException e) {
            System.err.println("Close wal raf error, slot: " + slot);
        }

        if (metaChunkSegmentFlagSeq != null) {
            metaChunkSegmentFlagSeq.cleanUp();
        }

        if (metaChunkSegmentIndex != null) {
            metaChunkSegmentIndex.cleanUp();
        }

        if (chunksArray != null) {
            for (var chunks : chunksArray) {
                for (var chunk : chunks) {
                    if (chunk != null) {
                        chunk.cleanUp();
                    }
                }
            }
        }

        if (readPersistedSegmentCache != null) {
            readPersistedSegmentCache.cleanUp();
            System.out.println("Cleanup read persisted pages cache");
        }

        for (var replPair : replPairs) {
            replPair.bye();
            replPair.close();
        }
    }

    private record PersistTaskParams(boolean isShortValue, int walGroupIndex, Wal targetWal, long submitTimeMillis) {

    }

    private class PersistTask implements RunnableEx {
        PersistTask(PersistTaskParams params, CompletableFuture<PersistTaskParams> cf) {
            this.params = params;
            this.cf = cf;
        }

        private final PersistTaskParams params;
        private final CompletableFuture<PersistTaskParams> cf;

        @Override
        public void run() {
            var targetWal = params.targetWal;

            boolean isError = false;
            try {
                if (params.isShortValue) {
                    var groupByBucketIndex = targetWal.delayToKeyBucketShortValues.values().stream()
                            .collect(Collectors.groupingBy(Wal.V::bucketIndex));
                    log.info("Batch update short value to key bucket, group by bucket index size: {}", groupByBucketIndex.size());

                    for (var entry : groupByBucketIndex.entrySet()) {
                        var bucketIndex = entry.getKey();
                        var list = entry.getValue();
                        keyLoader.persistShortValueListBatch(bucketIndex, list);
                    }
                } else {
                    var list = new ArrayList<>(targetWal.delayToKeyBucketValues.values());
                    // sort by bucket index for future merge better
                    list.sort(Comparator.comparingInt(Wal.V::bucketIndex));

                    var batchIndex = targetWal.batchIndex;
                    var workerId = list.get(0).workerId();
                    var chunk = chunksArray[workerId][batchIndex];

                    var needMergeSegmentIndexList = chunk.persist(list);
                    if (needMergeSegmentIndexList == null) {
                        isError = true;
                        return;
                    }

                    if (!needMergeSegmentIndexList.isEmpty()) {
                        chunkMerger.submit(workerId, slot, batchIndex, needMergeSegmentIndexList);
                    }
                }
            } catch (Exception e) {
                log.error("Persist Task error", e);
            } finally {
                if (!isError) {
                    cf.complete(params);
                }
            }
        }
    }

    private void submitPersistTaskFromWal(boolean isShortValue, int walGroupIndex, Wal targetWal) {
        var params = new PersistTaskParams(isShortValue, walGroupIndex, targetWal, System.currentTimeMillis());
        CompletableFuture<PersistTaskParams> cf = new CompletableFuture<>();
        var persistTask = new PersistTask(params, cf);

        // async
        cf.whenComplete((result, e) -> {
            if (e != null) {
                log.error("Persist Task error", e);
                throw new RuntimeException(e);
            } else {
                var costTimeMillis = System.currentTimeMillis() - result.submitTimeMillis;
                targetWal.persistCount++;
                targetWal.persistCostTimeMillis += costTimeMillis;

                if (targetWal.persistCount % 1000 == 0) {
                    log.info("Persist success, slot: {}, batch index: {}, persist count: {}, this time cost time millis: {}, avg cost time millis: {}",
                            slot, targetWal.batchIndex, targetWal.persistCount, costTimeMillis, targetWal.persistCostTimeMillis / targetWal.persistCount);
                }

                // add to queue for reuse
                walQueueArray[result.walGroupIndex].add(result.targetWal);
            }
        });
        var persistHandleEventloop = persistHandleEventloopArray[targetWal.batchIndex];
        persistHandleEventloop.submit(persistTask);
    }

    public TreeSet<ChunkMergeWorker.MergedSegment> getMergedSegmentSet(byte mergeWorkerId, byte workerId) {
        var chunkMergeWorker = chunkMerger.getChunkMergeWorker(mergeWorkerId);
        var mergedSegmentSets = chunkMergeWorker.mergedSegmentSets;

        var r = new TreeSet<ChunkMergeWorker.MergedSegment>();
        for (var mergedSegmentSet : mergedSegmentSets) {
            for (var one : mergedSegmentSet) {
                if (one.workerId() == workerId) {
                    r.add(one);
                }
            }
        }
        return r;
    }

    public long getMergeLastPersistAtMillis(byte mergeWorkerId) {
        var chunkMergeWorker = chunkMerger.getChunkMergeWorker(mergeWorkerId);
        return chunkMergeWorker.lastPersistAtMillis;
    }

    public int getMergeLastPersistedSegmentIndex(byte mergeWorkerId) {
        var chunkMergeWorker = chunkMerger.getChunkMergeWorker(mergeWorkerId);
        return chunkMergeWorker.lastPersistedSegmentIndex;
    }

    public Chunk.SegmentFlag getSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex) {
        return metaChunkSegmentFlagSeq.getSegmentMergeFlag(workerId, batchIndex, segmentIndex);
    }

    public void setSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex,
                                    byte flag, byte mergeWorkerId, long segmentSeq) {
        metaChunkSegmentFlagSeq.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, flag, mergeWorkerId, segmentSeq);
    }

    public List<Long> getSomeSegmentsSeqList(byte workerId, byte batchIndex, int segmentIndex, int segmentCount) {
        return metaChunkSegmentFlagSeq.getSomeSegmentsSeqList(workerId, batchIndex, segmentIndex, segmentCount);
    }

    public void setSegmentMergeFlagBatch(byte workerId, byte batchIndex, int segmentIndex, int segmentCount,
                                         byte flag, byte mergeWorkerId, List<Long> segmentSeqList) {
        var bytes = new byte[segmentCount * MetaChunkSegmentFlagSeq.ONE_LENGTH];
        var buffer = ByteBuffer.wrap(bytes);
        for (int i = 0; i < segmentCount; i++) {
            buffer.put(i * 2, flag);
            buffer.put(i * 2 + 1, mergeWorkerId);
            buffer.putLong(i * 2 + 2, segmentSeqList.get(i));
        }
        metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, bytes);
    }

    public void persistMergeSegmentsUndone() throws Exception {
        ArrayList<Integer>[][] needMergeSegmentIndexListArray = new ArrayList[allWorkers][batchNumber];
        for (int i = 0; i < allWorkers; i++) {
            for (int j = 0; j < batchNumber; j++) {
                needMergeSegmentIndexListArray[i][j] = new ArrayList<>();
            }
        }

        this.metaChunkSegmentFlagSeq.iterate((workerId, batchIndex, segmentIndex, flag, mergeWorkerId, segmentSeq) -> {
            if (flag == Chunk.SEGMENT_FLAG_MERGED || flag == Chunk.SEGMENT_FLAG_MERGING) {
                log.warn("Segment not persisted after merging, w={}, s={}, b={}, i={}, flag={}", workerId, slot, batchIndex, segmentIndex, flag);
                needMergeSegmentIndexListArray[workerId][batchIndex].add(segmentIndex);
            }
        });

        for (int workerId = 0; workerId < allWorkers; workerId++) {
            for (int batchIndex = 0; batchIndex < batchNumber; batchIndex++) {
                var needMergeSegmentIndexList = needMergeSegmentIndexListArray[workerId][batchIndex];
                if (!needMergeSegmentIndexList.isEmpty()) {
                    var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
                    var lastSegmentIndex = needMergeSegmentIndexList.getLast();

                    if (lastSegmentIndex - firstSegmentIndex + 1 == needMergeSegmentIndexList.size()) {
                        // wait until done
                        // write batch list duplicated if restart server
                        int validCvCountAfterRun = chunkMerger.submit((byte) workerId, slot, (byte) batchIndex, needMergeSegmentIndexList).get();
                        log.warn("Merge segments undone, w={}, s={}, b={}, i={}, end i={}, valid cv count after run: {}", workerId, slot, batchIndex,
                                firstSegmentIndex, lastSegmentIndex, validCvCountAfterRun);
                    } else {
                        // split
                        ArrayList<Integer> onceList = new ArrayList<>();
                        onceList.add(firstSegmentIndex);

                        int last = firstSegmentIndex;
                        for (int i = 1; i < needMergeSegmentIndexList.size(); i++) {
                            var segmentIndex = needMergeSegmentIndexList.get(i);
                            if (segmentIndex - last != 1) {
                                if (!onceList.isEmpty()) {
                                    int validCvCountAfterRun = chunkMerger.submit((byte) workerId, slot, (byte) batchIndex, onceList).get();
                                    log.warn("Merge segments undone, w={}, s={}, b={}, i={}, end i={} valid cv count after run: {}", workerId, slot, batchIndex,
                                            onceList.getFirst(), onceList.getLast(), validCvCountAfterRun);
                                    onceList.clear();
                                }
                            }
                            onceList.add(segmentIndex);
                            last = segmentIndex;
                        }

                        if (!onceList.isEmpty()) {
                            int validCvCountAfterRun = chunkMerger.submit((byte) workerId, slot, (byte) batchIndex, onceList).get();
                            log.warn("Merge segments undone, w={}, s={}, b={}, i={}, end i={} valid cv count after run: {}", workerId, slot, batchIndex,
                                    onceList.getFirst(), onceList.getLast(), validCvCountAfterRun);
                        }
                    }
                }
            }
        }
    }

    public int getChunkWriteSegmentIndex(byte workerId, byte batchIndex) {
        return metaChunkSegmentIndex.get(workerId, batchIndex);
    }

    public void setChunkWriteSegmentIndex(byte workerId, byte batchIndex, int segmentIndex) {
        metaChunkSegmentIndex.put(workerId, batchIndex, segmentIndex);
    }
}
