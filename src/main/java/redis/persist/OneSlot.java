package redis.persist;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.luben.zstd.Zstd;
import io.activej.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import jnr.posix.LibC;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.*;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static redis.persist.Chunk.SEGMENT_HEADER_LENGTH;

public class OneSlot implements OfStats {
    public OneSlot(byte slot, SnowFlake snowFlake, File persistDir, Config persistConfig) throws IOException {
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

        this.bigStringDir = new File(slotDir, "big-string");
        if (!bigStringDir.exists()) {
            if (!bigStringDir.mkdirs()) {
                throw new IOException("Create big string dir error, slot: " + slot);
            }
        }

        int bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;
        var walGroupNumber = bucketsPerSlot / ConfForSlot.global.confWal.oneChargeBucketNumber;
        this.walQueueArray = new LinkedBlockingQueue[walGroupNumber];
        this.walsArray = new Wal[walGroupNumber][];
        this.currentWalArray = new Wal[walGroupNumber];

        this.rafArray = new RandomAccessFile[batchNumber];
        this.rafShortValueArray = new RandomAccessFile[batchNumber];

        for (int i = 0; i < batchNumber; i++) {
            var walSharedFileBatch = new File(slotDir, "wal-" + i + ".dat");
            if (!walSharedFileBatch.exists()) {
                FileUtils.touch(walSharedFileBatch);

                for (int j = 0; j < walGroupNumber; j++) {
                    FileUtils.writeByteArrayToFile(walSharedFileBatch, Wal.M2, true);
                }
            }
            rafArray[i] = new RandomAccessFile(walSharedFileBatch, "rw");

            var walSharedFileShortValueBatch = new File(slotDir, "wal-short-value-" + i + ".dat");
            if (!walSharedFileShortValueBatch.exists()) {
                FileUtils.touch(walSharedFileShortValueBatch);

                for (int j = 0; j < walGroupNumber; j++) {
                    FileUtils.writeByteArrayToFile(walSharedFileShortValueBatch, Wal.M2, true);
                }
            }
            rafShortValueArray[i] = new RandomAccessFile(walSharedFileShortValueBatch, "rw");
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

        this.keyLoader = new KeyLoader(slot, slotDir);

        this.persistExecutors = new ExecutorService[batchNumber];
        for (int i = 0; i < batchNumber; i++) {
            var executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("wal-p-s-" + slot + "-b-" + i));
            persistExecutors[i] = executor;
        }

        this.compressStats = new CompressStats("slot-" + slot);

        this.initSegmentCache();
    }

    private void initSegmentCache() {
        var lru = ConfForSlot.global.confChunk.lru;
        this.readPersistedSegmentCache = Caffeine.newBuilder()
                .recordStats()
                .expireAfterWrite(lru.expireAfterWrite, TimeUnit.SECONDS)
                .expireAfterAccess(lru.expireAfterAccess, TimeUnit.SECONDS)
                .maximumWeight(lru.maximumBytes)
                .weigher((SegmentCacheKey k, byte[] v) -> v.length)
                .build();
        log.info("Read segment cache init, expire after write: {} s, expire after access: {} s, maximum bytes: {}",
                lru.expireAfterWrite, lru.expireAfterAccess, lru.maximumBytes);
    }


    private final Logger log = LoggerFactory.getLogger(OneSlot.class);

    private final byte slot;
    private final int segmentLength;
    private final int batchNumber;
    private final SnowFlake snowFlake;
    private final Config persistConfig;
    private final File slotDir;
    private final File bigStringDir;

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

    private final ExecutorService[] persistExecutors;

    private LibC libC;
    private byte allWorkers;
    private byte requestWorkers;
    private byte mergeWorkers;
    private byte topMergeWorkers;

    private ChunkMerger chunkMerger;

    public void setChunkMerger(ChunkMerger chunkMerger) {
        this.chunkMerger = chunkMerger;

        for (int i = requestWorkers; i < allWorkers; i++) {
            for (var chunk : chunksArray[i]) {
                chunkMerger.getChunkMergeWorker((byte) i).fixChunkThreadId(chunk);
            }
        }
    }

    // first index is worker id, second index is batch index
    Chunk[][] chunksArray;

    private long chunkMaxFdLength;

    private ChunkSegmentFlagMmapBuffer chunkSegmentFlagMmapBuffer;
    private ChunkSegmentIndexMmapBuffer chunkSegmentIndexMmapBuffer;

    private final CompressStats compressStats;

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
            return pread(segmentCacheKey.workerId, segmentCacheKey.batchIndex, segmentCacheKey.segmentIndex);
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

    void refreshReadPersistedSegmentCache(byte workerId, byte batchIndex, int segmentIndex, byte[] compressedBytesWithLength) {
        var k = new SegmentCacheKey(workerId, batchIndex, segmentIndex, 0);
        readPersistedSegmentCache.put(k, compressedBytesWithLength);
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

        var valueBytes = keyLoader.get(bucketIndex, keyBytes, keyHash);
        if (valueBytes == null) {
            return null;
        }

        // is not meta, short value
        if (!PersistValueMeta.isPvm(valueBytes)) {
            return Unpooled.wrappedBuffer(valueBytes);
        }

        var pvm = PersistValueMeta.decode(valueBytes);
        if (pvm.isExpired()) {
            return null;
        }

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
        int segmentIndex = (int) (pvm.offset / segmentLength);
        byte[] compressedBytesWithLength;
        if (!perfTestReadSegmentNoCache) {
            var segmentCacheKey = new SegmentCacheKey(pvm.workerId, pvm.batchIndex, segmentIndex, 0);
            compressedBytesWithLength = readPersistedSegmentCache.get(segmentCacheKey, fnLoadPersistedSegmentBytes);
        } else {
            // ignore big string, just for no cache, ssd read performance test
            compressedBytesWithLength = pread(pvm.workerId, pvm.batchIndex, segmentIndex);
        }
        if (compressedBytesWithLength == null) {
            throw new IllegalStateException("Load persisted segment bytes error, pvm: " + pvm);
        }

        var uncompressedBytes = new byte[segmentLength];
        long begin = System.nanoTime();
        // first 4 bytes is compressed length
        var d = Zstd.decompressByteArray(uncompressedBytes, 0, segmentLength,
                compressedBytesWithLength, 4, compressedBytesWithLength.length - 4);
        long costT = System.nanoTime() - begin;
        if (d != segmentLength) {
            throw new IllegalStateException("Decompress error, w=" + pvm.workerId + ", s=" + pvm.slot +
                    ", b=" + pvm.batchIndex + ", i=" + segmentIndex + ", d=" + d + ", segmentLength=" + segmentLength);
        }

        // thread safe, need not long adder
        compressStats.decompressCount2.increment();
        compressStats.decompressCostTotalTimeNanos2.add(costT);

        var fdOffset = pvm.offset % chunkMaxFdLength;

        var buf = Unpooled.wrappedBuffer(uncompressedBytes);
        buf.readerIndex((int) (fdOffset % segmentLength));
        return buf;
    }

    public boolean remove(int bucketIndex, String key, long keyHash) {
        boolean[] isDeletedArr = {false};
        keyLoader.bucketLock(bucketIndex, () -> {
            var isRemovedFromWal = removeFromWal(key, bucketIndex);
            isDeletedArr[0] = isRemovedFromWal || keyLoader.remove(bucketIndex, key.getBytes(), keyHash);
        });
        return isDeletedArr[0];
    }

    public void removeDelay(byte workerId, String key, int bucketIndex, long keyHash) {
        removeDelay(workerId, key, bucketIndex, keyHash, Long.MAX_VALUE);
    }

    public void removeDelay(byte workerId, String key, int bucketIndex, long keyHash, long seq) {
        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
        var putResult = currentWalArray[walGroupIndex].removeDelay(workerId, key, bucketIndex, keyHash, seq);

        if (putResult.needPersist()) {
            doPersist(walGroupIndex, key, putResult);
        }
    }

    private boolean removeFromWal(String key, int bucketIndex) {
        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;

        boolean isRemoved = false;
        for (var wal : walsArray[walGroupIndex]) {
            var isRemovedThisWal = wal.remove(key);
            if (isRemovedThisWal) {
                isRemoved = true;
            }
        }
        return isRemoved;
    }

    long threadIdProtected = -1;

    // thread safe, same slot, same event loop
    public void put(byte workerId, String key, int bucketIndex, CompressedValue cv) {
        var currentThreadId = Thread.currentThread().threadId();
        if (threadIdProtected != -1 && threadIdProtected != currentThreadId) {
            throw new IllegalStateException("Thread id not match, w=" + workerId + ", s=" + slot +
                    ", t=" + currentThreadId + ", t2=" + threadIdProtected);
        }

        byte[] cvEncoded;
        boolean isValueShort = cv.noExpire() && (cv.isNumber() || cv.isShortString());
        if (isValueShort) {
            if (cv.isNumber()) {
                cvEncoded = cv.encodeNumberWithType();
            } else {
                cvEncoded = cv.encodeShortString();
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
            var uuidAsFileName = String.valueOf(uuid);
            var file = new File(bigStringDir, uuidAsFileName);
            try {
                FileUtils.writeByteArrayToFile(file, cv.getCompressedData());
            } catch (IOException e) {
                throw new RuntimeException("Write big string error, key=" + key, e);
            }

            // encode again
            cvEncoded = cv.encodeAsBigStringMeta(uuid);
            v = new Wal.V(workerId, cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cvEncoded, cv.compressedLength());

            isValueShort = true;
        }

        var walGroupIndex = bucketIndex / ConfForSlot.global.confWal.oneChargeBucketNumber;
        var putResult = currentWalArray[walGroupIndex].put(isValueShort, key, v);
        if (!putResult.needPersist()) {
            return;
        }

        doPersist(walGroupIndex, key, putResult);
    }

    private void doPersist(int walGroupIndex, String key, Wal.PutResult putResult) {
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
            if (putResult.needPutV() != null) {
                nextAvailableWal.put(putResult.isValueShort(), key, putResult.needPutV());
            }

            submitPersistTask(putResult.isValueShort(), walGroupIndex, currentWalArray[walGroupIndex]);
            currentWalArray[walGroupIndex] = nextAvailableWal;
            nextAvailableWal.lastUsedTimeMillis = System.currentTimeMillis();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        for (var wals : walsArray) {
            for (var wal : wals) {
                wal.clear();
            }
        }

        this.keyLoader.flush();
        this.chunkSegmentFlagMmapBuffer.flush();
        this.chunkSegmentIndexMmapBuffer.flush();
        this.chunkMerger.flushTopChunkSegmentIndexMmapBuffer();
    }

    public void initChunks(LibC libC, byte allWorkers, byte requestWorkers, byte mergeWorkers, byte topMergeWorkers) throws IOException {
        this.allWorkers = allWorkers;
        this.requestWorkers = requestWorkers;
        this.mergeWorkers = mergeWorkers;
        this.topMergeWorkers = topMergeWorkers;

        this.libC = libC;
        this.keyLoader.init(libC, persistConfig);

        // meta data
        var chunkSegmentFlagFile = new File(slotDir, "segment-flag.map");
        this.chunkSegmentFlagMmapBuffer = new ChunkSegmentFlagMmapBuffer(chunkSegmentFlagFile, allWorkers);
        chunkSegmentFlagMmapBuffer.setLibC(libC);
        chunkSegmentFlagMmapBuffer.init();

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

        var chunkSegmentIndexFile = new File(slotDir, "segment-index.map");
        this.chunkSegmentIndexMmapBuffer = new ChunkSegmentIndexMmapBuffer(chunkSegmentIndexFile, allWorkers);
        chunkSegmentIndexMmapBuffer.setLibC(libC);
        chunkSegmentIndexMmapBuffer.init();

        // chunks
        this.chunksArray = new Chunk[allWorkers][batchNumber];
        for (int i = 0; i < allWorkers; i++) {
            var chunks = new Chunk[batchNumber];
            chunksArray[i] = chunks;

            var workerId = (byte) i;
            for (int j = 0; j < batchNumber; j++) {
                var chunk = new Chunk(workerId, slot, (byte) j, requestWorkers, snowFlake, slotDir, this, keyLoader);
                chunks[j] = chunk;

                initChunk(chunk);

                if (i < requestWorkers) {
                    fixRequestWorkerChunkThreadId(chunk);
                }
            }
        }

        this.chunkMaxFdLength = chunksArray[0][0].maxFdLength;
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
                setSegmentMergeFlag(chunk.workerId, chunk.batchIndex, chunk.segmentIndex, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, Chunk.MAIN_WORKER_ID);
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

    private void fixRequestWorkerChunkThreadId(Chunk chunk) {
        var executor = persistExecutors[chunk.batchIndex];
        executor.submit(() -> {
            chunk.threadIdProtected = Thread.currentThread().threadId();
            chunk.setWorkerType(true, false, false);
            log.warn("Fix request worker chunk chunk thread id, w={}, rw={}, s={}, b={}, tid={}",
                    chunk.workerId, chunk.workerId, slot, chunk.batchIndex, chunk.threadIdProtected);
        });
    }

    public int preadForMerge(byte workerId, byte batchIndex, int segmentIndex, ByteBuffer buffer, long offset) {
        var chunk = chunksArray[workerId][batchIndex];
        return chunk.preadForMerge(segmentIndex, buffer, offset);
    }

    public byte[] pread(byte workerId, byte batchIndex, int segmentIndex) {
        var chunk = chunksArray[workerId][batchIndex];
        return chunk.pread(segmentIndex);
    }

    public void cleanUp() {
        // executor shutdown before chunk clean up
        for (var executor : persistExecutors) {
            executor.shutdownNow();
        }
        System.out.println("Slot handler persist executor shutdown, slot: " + slot);

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

        if (chunkSegmentFlagMmapBuffer != null) {
            chunkSegmentFlagMmapBuffer.cleanUp();
        }

        if (chunkSegmentIndexMmapBuffer != null) {
            chunkSegmentIndexMmapBuffer.cleanUp();
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
    }

    private record PersistTaskParams(boolean isShortValue, int walGroupIndex, Wal targetWal, long submitTimeMillis) {

    }

    private class PersistTask implements Runnable {
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
                        chunkMerger.execute(workerId, slot, batchIndex, needMergeSegmentIndexList);
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

    private void submitPersistTask(boolean isShortValue, int walGroupIndex, Wal targetWal) {
        var params = new PersistTaskParams(isShortValue, walGroupIndex, targetWal, System.currentTimeMillis());
        CompletableFuture<PersistTaskParams> cf = new CompletableFuture<>();
        var persistTask = new PersistTask(params, cf);

        // sync
//        persistTask.run();
//        handlePersistResult(params);

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
        var executor = persistExecutors[targetWal.batchIndex];
        executor.submit(persistTask);
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
        var bytes = chunkSegmentFlagMmapBuffer.getSegmentMergeFlag(workerId, batchIndex, segmentIndex);
        if (bytes == null) {
            return null;
        }
        return new Chunk.SegmentFlag(bytes[0], bytes[1]);
    }

    public void setSegmentMergeFlag(byte workerId, byte batchIndex, int segmentIndex, byte flag, byte mergeWorkerId) {
        chunkSegmentFlagMmapBuffer.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, flag, mergeWorkerId);
    }

    public void setSegmentMergeFlagBatch(byte workerId, byte batchIndex, int segmentIndex, int segmentCount, byte flag, byte mergeWorkerId) {
        var bytes = new byte[segmentCount * 2];
        for (int i = 0; i < segmentCount; i++) {
            bytes[i * 2] = flag;
            bytes[i * 2 + 1] = mergeWorkerId;
        }
        chunkSegmentFlagMmapBuffer.setSegmentMergeFlagBatch(workerId, batchIndex, segmentIndex, bytes);
    }

    public void persistMergeSegmentsUndone() throws Exception {
        ArrayList<Integer>[][] needMergeSegmentIndexListArray = new ArrayList[allWorkers][batchNumber];
        for (int i = 0; i < allWorkers; i++) {
            for (int j = 0; j < batchNumber; j++) {
                needMergeSegmentIndexListArray[i][j] = new ArrayList<>();
            }
        }

        this.chunkSegmentFlagMmapBuffer.iterate((workerId, batchIndex, segmentIndex, flag, flagWorkerId) -> {
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
                        int validCvCountAfterRun = chunkMerger.execute((byte) workerId, slot, (byte) batchIndex, needMergeSegmentIndexList).get();
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
                                    int validCvCountAfterRun = chunkMerger.execute((byte) workerId, slot, (byte) batchIndex, onceList).get();
                                    log.warn("Merge segments undone, w={}, s={}, b={}, i={}, end i={} valid cv count after run: {}", workerId, slot, batchIndex,
                                            onceList.getFirst(), onceList.getLast(), validCvCountAfterRun);
                                    onceList.clear();
                                }
                            }
                            onceList.add(segmentIndex);
                            last = segmentIndex;
                        }

                        if (!onceList.isEmpty()) {
                            int validCvCountAfterRun = chunkMerger.execute((byte) workerId, slot, (byte) batchIndex, onceList).get();
                            log.warn("Merge segments undone, w={}, s={}, b={}, i={}, end i={} valid cv count after run: {}", workerId, slot, batchIndex,
                                    onceList.getFirst(), onceList.getLast(), validCvCountAfterRun);
                        }
                    }
                }
            }
        }
    }

    public int getChunkWriteSegmentIndex(byte workerId, byte batchIndex) {
        return chunkSegmentIndexMmapBuffer.get(workerId, batchIndex);
    }

    public void setChunkWriteSegmentIndex(byte workerId, byte batchIndex, int segmentIndex) {
        chunkSegmentIndexMmapBuffer.put(workerId, batchIndex, segmentIndex);
    }
}
