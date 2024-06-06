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
import redis.command.XGroup;
import redis.metric.SimpleGauge;
import redis.repl.MasterUpdateCallback;
import redis.repl.ReplPair;
import redis.repl.SendToSlaveMasterUpdateCallback;
import redis.repl.content.ToMasterExistsSegmentMeta;
import redis.repl.content.ToSlaveWalAppendBatch;
import redis.task.ITask;
import redis.task.TaskChain;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static redis.persist.Chunk.ONCE_PREPARE_SEGMENT_COUNT;
import static redis.persist.Chunk.SEGMENT_HEADER_LENGTH;

public class OneSlot {
    // for unit test
    public OneSlot(byte slot, File slotDir, KeyLoader keyLoader, Wal wal) throws IOException {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotDir = slotDir;
        this.slotNumber = 1;

        this.keyLoader = keyLoader;
        this.snowFlake = new SnowFlake(1, 1);
        this.persistConfig = Config.create();
        this.segmentLength = 4096;

        this.bigStringFiles = null;
        this.chunkMergeWorker = null;
        this.dynConfig = null;
        this.walArray = new Wal[]{wal};
        this.raf = null;
        this.rafShortValue = null;
        this.masterUpdateCallback = null;
        this.masterUuid = 0L;

        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);
    }

    public OneSlot(byte slot, short slotNumber, SnowFlake snowFlake, File persistDir, Config persistConfig) throws IOException {
        this.segmentLength = ConfForSlot.global.confChunk.segmentLength;

        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.slotNumber = slotNumber;
        this.snowFlake = snowFlake;
        this.persistConfig = persistConfig;

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

        var walGroupNumber = Wal.calcWalGroupNumber();
        this.walArray = new Wal[walGroupNumber];

        var walSharedFile = new File(slotDir, "wal.dat");
        if (!walSharedFile.exists()) {
            FileUtils.touch(walSharedFile);

            var initTimes = walGroupNumber / Wal.INIT_M4_TIMES;
            for (int j = 0; j < initTimes; j++) {
                FileUtils.writeByteArrayToFile(walSharedFile, Wal.INIT_M4, true);
            }
        }
        this.raf = new RandomAccessFile(walSharedFile, "rw");
        var lruMemoryRequireMBWriteInWal = walSharedFile.length() / 1024 / 1024;
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, (int) lruMemoryRequireMBWriteInWal, false);

        var walSharedFileShortValue = new File(slotDir, "wal-short-value.dat");
        if (!walSharedFileShortValue.exists()) {
            FileUtils.touch(walSharedFileShortValue);

            var initTimes = walGroupNumber / Wal.INIT_M4_TIMES;
            for (int j = 0; j < initTimes; j++) {
                FileUtils.writeByteArrayToFile(walSharedFileShortValue, Wal.INIT_M4, true);
            }
        }
        this.rafShortValue = new RandomAccessFile(walSharedFileShortValue, "rw");
        var lruMemoryRequireMBWriteInWal2 = walSharedFileShortValue.length() / 1024 / 1024;
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_write_in_wal, (int) lruMemoryRequireMBWriteInWal2, false);

        for (int i = 0; i < walGroupNumber; i++) {
            var wal = new Wal(slot, i, raf, rafShortValue, snowFlake);
            walArray[i] = wal;
        }

        // cache lru
        int maxSizeForAllWalGroups = ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize;
        var maxSizeForEachWalGroup = maxSizeForAllWalGroups / walGroupNumber;
        final var maybeOneCompressedValueEncodedLength = 200;
        var lruMemoryRequireMBReadGroupByWalGroup = maxSizeForAllWalGroups * maybeOneCompressedValueEncodedLength / 1024 / 1024;
        log.info("LRU max size for each wal group: {}, all wal group number: {}, maybe one compressed value encoded length is {}B, memory require: {}MB",
                maxSizeForEachWalGroup,
                walGroupNumber,
                maybeOneCompressedValueEncodedLength,
                lruMemoryRequireMBReadGroupByWalGroup);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.kv_read_group_by_wal_group, lruMemoryRequireMBReadGroupByWalGroup, false);

        for (int walGroupIndex = 0; walGroupIndex < walGroupNumber; walGroupIndex++) {
            LRUMap<String, byte[]> lru = new LRUMap<>(maxSizeForEachWalGroup);
            kvByWalGroupIndexLRU.put(walGroupIndex, lru);
        }

        // default 2000, I do not know if it is suitable
        var sendOnceMaxCount = persistConfig.get(ofInteger(), "repl.wal.sendOnceMaxCount", 2000);
        var sendOnceMaxSize = persistConfig.get(ofInteger(), "repl.wal.sendOnceMaxSize", 1024 * 1024);
        var toSlaveWalAppendBatch = new ToSlaveWalAppendBatch(sendOnceMaxCount, sendOnceMaxSize);
        // sync / async to slave callback
        this.masterUpdateCallback = new SendToSlaveMasterUpdateCallback(() -> replPairs.stream().
                filter(ReplPair::isAsMaster).collect(Collectors.toList()), toSlaveWalAppendBatch);

        this.keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, this);

        DictMap.getInstance().setMasterUpdateCallback(masterUpdateCallback);

        this.initTasks();
        this.initMetricsCollect();
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
        replPair.initAsSlave(netWorkerEventloop, requestHandler);
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
                replPair1.initAsMaster(slaveUuid, netWorkerEventloop, requestHandler);
                return replPair1;
            }
        }

        replPair.initAsMaster(slaveUuid, netWorkerEventloop, requestHandler);
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
        return Promise.ofFuture(netWorkerEventloop.submit(runnableEx));
    }

    public <T> Promise<T> asyncCall(SupplierEx<T> supplierEx) {
        return Promise.ofFuture(netWorkerEventloop.submit(AsyncComputation.of(supplierEx)));
    }

    private final byte slot;
    private final String slotStr;
    private final short slotNumber;

    public byte slot() {
        return slot;
    }

    private final int segmentLength;
    private final SnowFlake snowFlake;
    private final Config persistConfig;
    final File slotDir;

    private final BigStringFiles bigStringFiles;

    public BigStringFiles getBigStringFiles() {
        return bigStringFiles;
    }

    private final Map<Integer, LRUMap<String, byte[]>> kvByWalGroupIndexLRU = new HashMap<>();

    void clearKvLRUByWalGroupIndex(int walGroupIndex) {
        var lru = kvByWalGroupIndexLRU.get(walGroupIndex);
        if (lru != null) {
            lru.clear();
            if (walGroupIndex == 0) {
                log.info("KV LRU cleared for wal group index: {}, I am alive, act normal", walGroupIndex);
            }
        }
    }

    private long kvLRUHitTotal = 0;
    private long kvLRUMissTotal = 0;

    final ChunkMergeWorker chunkMergeWorker;

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
        return bigStringFiles.bigStringDir;
    }

    // index is group index
    private final Wal[] walArray;

    private final RandomAccessFile raf;
    private final RandomAccessFile rafShortValue;

    final KeyLoader keyLoader;

    public KeyLoader getKeyLoader() {
        return keyLoader;
    }

    private final MasterUpdateCallback masterUpdateCallback;

    public long getWalKeyCount() {
        long r = 0;
        for (var wal : walArray) {
            r += wal.getKeyCount();
        }
        return r;
    }

    public long getAllKeyCount() {
        return keyLoader.getKeyCount() + getWalKeyCount();
    }

    private LibC libC;


    private byte netWorkers;

    Chunk chunk;

    MetaChunkSegmentFlagSeq metaChunkSegmentFlagSeq;

    public byte[] getMetaChunkSegmentFlagSeqBytesToSlaveExists() {
        return metaChunkSegmentFlagSeq.getInMemoryCachedBytes();
    }

    public void overwriteMetaChunkSegmentFlagSeqBytesFromMasterExists(byte[] bytes) {
        metaChunkSegmentFlagSeq.overwriteInMemoryCachedBytes(bytes);
    }

    MetaChunkSegmentIndex metaChunkSegmentIndex;

    // read only, important
    public byte[] getMetaChunkSegmentIndexBytesToSlaveExists() {
        return metaChunkSegmentIndex.getInMemoryCachedBytes();
    }

    public void overwriteMetaChunkSegmentIndexBytesFromRepl(byte[] bytes) {
        metaChunkSegmentIndex.overwriteInMemoryCachedBytes(bytes);
    }

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

    public record BufOrCompressedValue(ByteBuf buf, CompressedValue cv) {
    }

    public BufOrCompressedValue get(byte[] keyBytes, int bucketIndex, long keyHash) {
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
            return new BufOrCompressedValue(Unpooled.wrappedBuffer(cvEncodedBytesFromLRU), null);
        }
        kvLRUMissTotal++;

        var valueBytesWithExpireAt = keyLoader.getValueByKey(bucketIndex, keyBytes, keyHash);
        if (valueBytesWithExpireAt == null) {
            return null;
        }

        var valueBytes = valueBytesWithExpireAt.valueBytes();
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
        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength < 0) {
            throw new IllegalStateException("Key length error, key length: " + keyLength);
        }

        var keyBytesRead = new byte[keyLength];
        buf.readBytes(keyBytesRead);

        if (!Arrays.equals(keyBytesRead, keyBytes)) {
            throw new IllegalStateException("Key not match, key: " + new String(keyBytes) + ", key persisted: " + new String(keyBytesRead));
        }

        // set to lru cache, just target bytes
        var cv = CompressedValue.decode(buf, keyBytes, keyHash, false);
        lru.put(key, cv.encode());

        return new BufOrCompressedValue(null, cv);
    }

    byte[] getFromWal(String key, int bucketIndex) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        return targetWal.get(key);
    }

    private byte[] getSegmentSubBlockDecompressedBytesByPvm(PersistValueMeta pvm) {
        byte[] tightBytesWithLength = chunk.preadSegmentTightBytesWithLength(pvm.segmentIndex);
        if (tightBytesWithLength == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm: " + pvm);
        }

        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        buffer.position(SegmentBatch.subBlockMetaPosition(pvm.subBlockIndex));
        var subBlockOffset = buffer.getShort();
        var subBlockLength = buffer.getShort();

        // memory copy
//        var compressedBytes = new byte[subBlockLength];
//        buffer.position(subBlockOffset).get(compressedBytes);

        var decompressedBytes = new byte[segmentLength];

        var beginT = System.nanoTime();
//        var decompressedBytes = Zstd.decompress(compressedBytes, segmentLength);
        var d = Zstd.decompressByteArray(decompressedBytes, 0, segmentLength,
                tightBytesWithLength, subBlockOffset, subBlockLength);
        var costT = (System.nanoTime() - beginT) / 1000;
        if (costT == 0) {
            costT = 1;
        }
        segmentDecompressTimeTotalUs += costT;
        segmentDecompressCountTotal++;

        if (d != segmentLength) {
            throw new IllegalStateException("Decompress error, s=" + pvm.slot +
                    ", i=" + pvm.segmentIndex + ", sbi=" + pvm.subBlockIndex + ", d=" + d + ", segmentLength=" + segmentLength);
        }

        return decompressedBytes;
    }

    public boolean remove(int bucketIndex, String key, long keyHash, boolean isDelayUpdateKeyBucket) {
        if (isDelayUpdateKeyBucket) {
            removeDelay(key, bucketIndex, keyHash);
            return true;
        }

        var isRemovedFromWal = removeFromWal(bucketIndex, key, keyHash);
        if (isRemovedFromWal) {
            return true;
        }

        var valueBytesWithExpireAtAndSeq = keyLoader.getValueByKey(bucketIndex, key.getBytes(), keyHash);
        if (valueBytesWithExpireAtAndSeq == null || valueBytesWithExpireAtAndSeq.isExpired()) {
            return false;
        }

        removeDelay(key, bucketIndex, keyHash);
        return true;
    }

    public void removeDelay(String key, int bucketIndex, long keyHash) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        var putResult = targetWal.removeDelay(key, bucketIndex, keyHash);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, bucketIndex, putResult);
        } else {
            if (masterUpdateCallback != null) {
                masterUpdateCallback.onWalAppend(slot, bucketIndex, putResult.isValueShort(), putResult.needPutV(), putResult.offset());
            }
        }
    }

    private boolean removeFromWal(int bucketIndex, String key, long keyHash) {
        var walGroupIndex = Wal.calWalGroupIndex(bucketIndex);
        var targetWal = walArray[walGroupIndex];
        boolean isRemoved = targetWal.remove(key);
        if (isRemoved) {
            removeDelay(key, bucketIndex, keyHash);
        }
        return isRemoved;
    }

    long threadIdProtectedWhenPut = -1;

    // thread safe, same slot, same event loop
    public void put(String key, int bucketIndex, CompressedValue cv) {
        var threadId = Thread.currentThread().getId();
        if (threadId != threadIdProtectedWhenPut) {
            throw new IllegalStateException("Thread id not match, thread id: " + threadId + ", thread id protected: " + threadIdProtectedWhenPut);
        }

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
                key, cvEncoded, cv.compressedLength());

        // for big string, use single file
        boolean isPersistLengthOverSegmentLength = v.persistLength() + SEGMENT_HEADER_LENGTH > segmentLength;
        if (isPersistLengthOverSegmentLength || key.startsWith("kerry-test-big-string-")) {
            var uuid = snowFlake.nextId();
            var bytes = cv.getCompressedData();
            bigStringFiles.writeBigStringBytes(uuid, key, bytes);

            if (masterUpdateCallback != null) {
                masterUpdateCallback.onBigStringFileWrite(slot, uuid, bytes);
            }

            // encode again
            cvEncoded = cv.encodeAsBigStringMeta(uuid);
            v = new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cvEncoded, cv.compressedLength());

            isValueShort = true;
        }

        var putResult = targetWal.put(isValueShort, key, v);
        if (!putResult.needPersist()) {
            if (masterUpdateCallback != null) {
                masterUpdateCallback.onWalAppend(slot, bucketIndex, isValueShort, v, putResult.offset());
            }

            return;
        }

        doPersist(walGroupIndex, key, bucketIndex, putResult);
    }

    private void doPersist(int walGroupIndex, String key, int bucketIndex, Wal.PutResult putResult) {
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
            if (masterUpdateCallback != null) {
                masterUpdateCallback.onWalAppend(slot, bucketIndex, putResult.isValueShort(), needPutV, putResult.offset());
            }
        }
    }

    public void asSlaveOnMasterWalAppendBatchGet(TreeMap<Integer, ArrayList<XGroup.ExtV>> extVsGroupByWalGroupIndex) {
        for (var entry : extVsGroupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var extVs = entry.getValue();

            for (var extV : extVs) {
                var wal = walArray[walGroupIndex];

                var offset = extV.offset();
                if (offset == 0) {
                    // clear
                    if (extV.isValueShort()) {
                        wal.delayToKeyBucketShortValues.clear();
                    } else {
                        wal.delayToKeyBucketValues.clear();
                    }
                }

                var v = extV.v();
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

    private LinkedList<ToMasterExistsSegmentMeta.OncePull> oncePulls = new LinkedList<>();

    public void resetOncePulls(LinkedList<ToMasterExistsSegmentMeta.OncePull> oncePulls) {
        this.oncePulls = oncePulls;
    }

    public ToMasterExistsSegmentMeta.OncePull removeOncePull(int beginSegmentIndex) {
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
        for (var wal : walArray) {
            wal.clear();
        }

        try {
            this.keyLoader.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.metaChunkSegmentFlagSeq.clear();
        this.metaChunkSegmentIndex.clear();
    }

    public void initFds(LibC libC, byte netWorkers) throws IOException {
        this.netWorkers = netWorkers;

        this.libC = libC;
        this.keyLoader.initFds(libC);

        // meta data
        this.metaChunkSegmentFlagSeq = new MetaChunkSegmentFlagSeq(slot, slotDir);
        this.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, slotDir);

        // chunk
        initChunk();
    }

    private void initChunk() throws IOException {
        this.chunk = new Chunk(slot, slotDir, this, snowFlake, keyLoader, masterUpdateCallback);
        chunk.initFds(libC);

        var segmentIndexLastSaved = metaChunkSegmentIndex.get();

        // write index mmap crash recovery
        boolean isBreak = false;
        for (int i = 0; i < ONCE_PREPARE_SEGMENT_COUNT; i++) {
            boolean canWrite = chunk.initSegmentIndexWhenFirstStart(segmentIndexLastSaved + i);
            // when restart server, set persisted flag
            if (!canWrite) {
                int currentSegmentIndex = chunk.currentSegmentIndex();
                log.warn("Segment can not write, s={}, i={}", slot, currentSegmentIndex);

                // set persisted flag, for next loop reuse
                setSegmentMergeFlag(currentSegmentIndex, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, snowFlake.nextId());
                log.warn("Reset persisted when init");

                chunk.moveSegmentIndexNext(1);
                setChunkWriteSegmentIndex(currentSegmentIndex);

                log.warn("Move to next segment, s={}, i={}", slot, currentSegmentIndex);
            } else {
                isBreak = true;
                break;
            }
        }

        if (!isBreak) {
            throw new IllegalStateException("Segment can not write after reset flag, s=" + slot + ", i=" + chunk.currentSegmentIndex());
        }
    }

    public void writeSegmentsFromMasterExists(int segmentIndex, int segmentCount, List<Long> segmentSeqList, byte[] bytes) {
        if (bytes.length != chunk.segmentLength * segmentCount) {
            throw new IllegalStateException("Bytes length not match, bytes length: " + bytes.length +
                    ", segment length: " + chunk.segmentLength + ", segment count: " + segmentCount);
        }

        chunk.writeSegmentsFromMasterExists(bytes, segmentIndex, segmentCount, segmentSeqList, bytes.length);
    }

    byte[] preadForMerge(int segmentIndex) {
        return chunk.preadForMerge(segmentIndex);
    }

    public byte[] preadForRepl(int segmentIndex) {
        return chunk.preadForRepl(segmentIndex);
    }

    public void cleanUp() {
        // close wal raf
        try {
            raf.close();
            System.out.println("Close wal raf success, slot: " + slot);

            rafShortValue.close();
            System.out.println("Close wal short value raf success, slot: " + slot);
        } catch (IOException e) {
            System.err.println("Close wal raf / wal short raf error, slot: " + slot);
        }

        if (metaChunkSegmentFlagSeq != null) {
            metaChunkSegmentFlagSeq.cleanUp();
        }

        if (metaChunkSegmentIndex != null) {
            metaChunkSegmentIndex.cleanUp();
        }

        chunk.cleanUp();

        for (var replPair : replPairs) {
            replPair.bye();
            replPair.close();
        }
    }

    private void persistWal(boolean isShortValue, Wal targetWal) {
        if (isShortValue) {
            keyLoader.persistShortValueListBatchInOneWalGroup(targetWal.groupIndex, targetWal.delayToKeyBucketShortValues.values());
        } else {
            var list = new ArrayList<>(targetWal.delayToKeyBucketValues.values());
            // sort by bucket index for future merge better
            list.sort(Comparator.comparingInt(Wal.V::bucketIndex));

            var needMergeSegmentIndexList = chunk.persist(targetWal.groupIndex, list, false);
            if (needMergeSegmentIndexList == null) {
                throw new IllegalStateException("Persist error, need merge segment index list is null, slot: " + slot);
            }

            if (!needMergeSegmentIndexList.isEmpty()) {
                doMergeJob(needMergeSegmentIndexList);
            }
        }
    }

    Chunk.SegmentFlag getSegmentMergeFlag(int segmentIndex) {
        return metaChunkSegmentFlagSeq.getSegmentMergeFlag(segmentIndex);
    }

    public List<Long> getSegmentMergeFlagListBatchForRepl(int segmentIndex, int segmentCount) {
        return metaChunkSegmentFlagSeq.getSegmentSeqListBatchForRepl(segmentIndex, segmentCount);
    }

    void setSegmentMergeFlag(int segmentIndex, byte flag, long segmentSeq) {
        metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex, flag, segmentSeq);
    }

    void setSegmentMergeFlagBatch(int segmentIndex, int segmentCount, byte flag, List<Long> segmentSeqList) {
        metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(segmentIndex, segmentCount, flag, segmentSeqList);
    }

    int doMergeJob(ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake);
        return job.run();
    }

    public void persistMergeSegmentsUndone() {
        ArrayList<Integer> needMergeSegmentIndexList = new ArrayList<>();

        final int[] lastMergedAndPersistSegmentIndexArray = {-1};
        this.metaChunkSegmentFlagSeq.iterate((segmentIndex, flag, segmentSeq) -> {
            if (flag == Chunk.SEGMENT_FLAG_MERGED || flag == Chunk.SEGMENT_FLAG_MERGING) {
                log.warn("Segment not persisted after merging, s={}, i={}, flag={}", slot, segmentIndex, flag);
                needMergeSegmentIndexList.add(segmentIndex);
            }

            if (flag == Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED) {
                lastMergedAndPersistSegmentIndexArray[0] = segmentIndex;
            }
        });

        if (needMergeSegmentIndexList.isEmpty()) {
            chunk.needMergeSegmentIndexEndLastTime = lastMergedAndPersistSegmentIndexArray[0];
            return;
        }

        var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();

        if (lastSegmentIndex - firstSegmentIndex + 1 == needMergeSegmentIndexList.size()) {
            var validCvCount = doMergeJob(needMergeSegmentIndexList);
            log.warn("Merge segments undone, s={}, i={}, end i={}, valid cv count after run: {}",
                    slot, firstSegmentIndex, lastSegmentIndex, validCvCount);
        } else {
            // split
            ArrayList<Integer> onceList = new ArrayList<>();
            onceList.add(firstSegmentIndex);

            int last = firstSegmentIndex;
            for (int i = 1; i < needMergeSegmentIndexList.size(); i++) {
                var segmentIndex = needMergeSegmentIndexList.get(i);
                if (segmentIndex - last != 1) {
                    if (!onceList.isEmpty()) {
                        doMergeJobOnceList(onceList);
                        onceList.clear();
                    }
                }
                onceList.add(segmentIndex);
                last = segmentIndex;
            }

            if (!onceList.isEmpty()) {
                doMergeJobOnceList(onceList);
            }
        }

        chunk.needMergeSegmentIndexEndLastTime = lastSegmentIndex;
    }

    private void doMergeJobOnceList(ArrayList<Integer> onceList) {
        var validCvCount = doMergeJob(onceList);
        log.warn("Merge segments undone, s={}, i={}, end i={} valid cv count after run: {}",
                slot, onceList.get(0), onceList.get(onceList.size() - 1), validCvCount);
    }

    public void setChunkWriteSegmentIndex(int segmentIndex) {
        metaChunkSegmentIndex.set(segmentIndex);
    }

    // metrics
    private final static SimpleGauge walDelaySizeGauge = new SimpleGauge("wal_delay_size", "wal delay size",
            "slot", "group_index");

    private final static SimpleGauge slotInnerGauge = new SimpleGauge("slot_inner", "slot inner",
            "slot");

    static {
        walDelaySizeGauge.register();
        slotInnerGauge.register();
    }

    private long segmentDecompressTimeTotalUs = 0;
    private long segmentDecompressCountTotal = 0;

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
            map.put("chunk_current_segment_index", new SimpleGauge.ValueWithLabelValues((double) chunk.currentSegmentIndex(), labelValues));

            if (slot == 0) {
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

                map.put("lru_prepare_mb_all", new SimpleGauge.ValueWithLabelValues(
                        (double) LRUPrepareBytesStats.sum(), labelValues));
            }

            map.put("kv_lru_hit_total", new SimpleGauge.ValueWithLabelValues((double) kvLRUHitTotal, labelValues));
            map.put("kv_lru_miss_total", new SimpleGauge.ValueWithLabelValues((double) kvLRUMissTotal, labelValues));

            map.put("segment_decompress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) segmentDecompressTimeTotalUs, labelValues));
            map.put("segment_decompress_count_total", new SimpleGauge.ValueWithLabelValues((double) segmentDecompressCountTotal, labelValues));
            if (segmentDecompressCountTotal > 0) {
                double segmentDecompressedCostTAvg = (double) segmentDecompressTimeTotalUs / segmentDecompressCountTotal;
                map.put("segment_decompress_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues(segmentDecompressedCostTAvg, labelValues));
            }

            var replPairSize = replPairs.stream().filter(one -> !one.isSendBye()).count();
            map.put("repl_pair_size", new SimpleGauge.ValueWithLabelValues((double) replPairSize, labelValues));
            return map;
        });
    }
}
