package redis.persist;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import io.activej.config.Config;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressStats;
import redis.ConfForSlot;
import redis.SnowFlake;
import redis.repl.MasterUpdateCallback;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static redis.persist.KeyBucket.AFTER_COMPRESS_PREPEND_LENGTH;
import static redis.persist.LocalPersist.PAGE_SIZE;
import static redis.persist.LocalPersist.PROTECTION;

public class KeyLoader implements OfStats {
    private static final int PAGE_NUMBER_PER_BUCKET = 1;
    static final int KEY_BUCKET_ONE_COST_SIZE = PAGE_NUMBER_PER_BUCKET * PAGE_SIZE;

    // one split file max 2GB, 2 * 1024 * 1024 / 4 = 524288
    // one split index one file
    static final int KEY_BUCKET_COUNT_PER_FD = 2 * 1024 * 1024 / 4;

    public KeyLoader(byte slot, int bucketsPerSlot, File slotDir, SnowFlake snowFlake,
                     MasterUpdateCallback masterUpdateCallback, DynConfig dynConfig) throws IOException {
        this.slot = slot;
        this.bucketsPerSlot = bucketsPerSlot;
        this.slotDir = slotDir;
        this.snowFlake = snowFlake;
        this.masterUpdateCallback = masterUpdateCallback;

        this.dynConfig = dynConfig;
    }

    private final byte slot;
    private final int bucketsPerSlot;
    private final File slotDir;
    private final SnowFlake snowFlake;
    private final MasterUpdateCallback masterUpdateCallback;

    private final DynConfig dynConfig;

    // use read write better than synchronized
    private MetaKeyBucketSplitNumber metaKeyBucketSplitNumber;

    public byte maxSplitNumber() {
        return metaKeyBucketSplitNumber.maxSplitNumber();
    }

    // read only, important
    public byte[] getMetaKeyBucketSplitNumberBytesToSlaveExists() {
        return metaKeyBucketSplitNumber.getInMemoryCachedBytes();
    }

    public void overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(byte[] bytes) {
        metaKeyBucketSplitNumber.overwriteInMemoryCachedBytes(bytes);
    }

    public void setMetaKeyBucketSplitNumberFromMasterNewly(int bucketIndex, byte splitNumber) {
        metaKeyBucketSplitNumber.set(bucketIndex, splitNumber);
    }

    // split 3 times, 3 * 3 * 3 = 27
    // when 27, batch persist pvm, will slot lock and read all 27 key buckets for target bucket index, write perf bad
    // read perf ok, because only read one key bucket and lru cache
    // increase buckets per slot config value, then will split fewer times, but will cost more memory
    public static final byte MAX_SPLIT_NUMBER = 27;
    static final int SPLIT_MULTI_STEP = 3;

    // index is bucket index
    private ReadWriteLock[] rwlArray;
    private Cache<KeyBucketCacheKey, byte[]> readPersistedKeyBucketCache;

    private final CompressStats compressStats = new CompressStats("key bucket");

    private LibC libC;
    private int[] fds;
    private ByteBuffer[] readBucketBuffers;
    private long[] readBucketBufferAddresses;

    public static final int READ_BUCKET_FOR_REPL_BATCH_NUMBER = 1024;
    private ByteBuffer[] readBucketBuffersForRepl;
    private long[] readBucketBufferAddressesForRepl;

    private static final int WRITE_BUCKET_BUFFER_GROUP_NUMBER = 1024;
    // first index is split index, second index is bucket index group
    private ByteBuffer[][] writeBucketBuffersArray;
    private long[][] writeBucketBufferAddressesArray;

    private final PageManager pageManager = PageManager.getInstance();

    private long pwriteCount;
    private long pwriteCostNanos;

    private long splitCount;
    private long splitCostNanos;

    private StatKeyBucketLastUpdateCount statKeyBucketLastUpdateCount;

    public short getKeyCountInBucketIndex(int bucketIndex) {
        return statKeyBucketLastUpdateCount.getKeyCountInBucketIndex(bucketIndex);
    }

    public long getKeyCount() {
        return statKeyBucketLastUpdateCount.getKeyCount();
    }

    // first index is split index, second index is bucket index
    private byte[][][] allInMemoryBytes;

    private final Logger log = LoggerFactory.getLogger(KeyLoader.class);

    public void init(LibC libC, Config persistConfig) throws IOException {
        this.rwlArray = new ReadWriteLock[bucketsPerSlot];
        for (int i = 0; i < bucketsPerSlot; i++) {
            this.rwlArray[i] = new ReentrantReadWriteLock();
        }

        var toInt = ofInteger();
        this.compressStats.initKeySizeByBucketLru(
                persistConfig.get(toInt, "keyBucket.stats.expireAfterWrite", 3600),
                persistConfig.get(toInt, "keyBucket.stats.expireAfterAccess", 3600),
                persistConfig.get(toInt, "keyBucket.stats.maximumSize", KeyBucket.MAX_BUCKETS_PER_SLOT));

        this.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, bucketsPerSlot, slotDir);
        this.statKeyBucketLastUpdateCount = new StatKeyBucketLastUpdateCount(slot, bucketsPerSlot, slotDir);

        if (ConfForSlot.global.pureMemory) {
            this.allInMemoryBytes = new byte[MAX_SPLIT_NUMBER][bucketsPerSlot][];
        } else {
            this.libC = libC;

            var lru = ConfForSlot.global.confBucket.lru;
            this.readPersistedKeyBucketCache = Caffeine.newBuilder()
                    .recordStats()
                    .expireAfterWrite(lru.expireAfterWrite, TimeUnit.SECONDS)
                    .expireAfterAccess(lru.expireAfterAccess, TimeUnit.SECONDS)
                    .maximumWeight(lru.maximumBytes)
                    .weigher((KeyBucketCacheKey k, byte[] v) -> v.length)
                    .build();
            log.info("Persisted key bucket cache init, expire after write: {} s, expire after access: {} s, maximum bytes: {}. slot: {}",
                    lru.expireAfterWrite, lru.expireAfterAccess, lru.maximumBytes, slot);

            int fdLength = MAX_SPLIT_NUMBER;
            this.fds = new int[fdLength];

            this.readBucketBuffers = new ByteBuffer[fdLength];
            this.readBucketBuffersForRepl = new ByteBuffer[fdLength];
            this.writeBucketBuffersArray = new ByteBuffer[fdLength][WRITE_BUCKET_BUFFER_GROUP_NUMBER];

            this.readBucketBufferAddresses = new long[fdLength];
            this.readBucketBufferAddressesForRepl = new long[fdLength];
            this.writeBucketBufferAddressesArray = new long[fdLength][WRITE_BUCKET_BUFFER_GROUP_NUMBER];

            var maxSplitNumber = metaKeyBucketSplitNumber.maxSplitNumber();
            initFdForSlot(maxSplitNumber);
        }
    }

    // need thread safe
    private synchronized void initFdForSlot(byte splitNumber) {
        if (ConfForSlot.global.pureMemory) {
            return;
        }

        // 4M
        var batchN = KEY_BUCKET_ONE_COST_SIZE * 1024;
        var batchCount = bucketsPerSlot / 1024;
        var bytes0 = new byte[batchN];

        long n = bucketsPerSlot * KEY_BUCKET_ONE_COST_SIZE;

        var m = MemoryIO.getInstance();
        for (int fdIndex = 0; fdIndex < splitNumber; fdIndex++) {
            var oneFile = new File(slotDir, "key-bucket-split-" + fdIndex + ".dat");
            if (!oneFile.exists()) {
                try {
                    FileUtils.touch(oneFile);

                    // init 0
                    for (int j = 0; j < batchCount; j++) {
                        FileUtils.writeByteArrayToFile(oneFile, bytes0, true);
                    }

                    log.warn("Create key bucket file, slot: {}, split: {}, length: {}", slot, fdIndex, n);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // already opened
            if (fds[fdIndex] != 0) {
                continue;
            }

            fds[fdIndex] = libC.open(oneFile.getAbsolutePath(), LocalPersist.O_DIRECT | OpenFlags.O_RDWR.value(), 00644);

            var addr = pageManager.allocatePages(PAGE_NUMBER_PER_BUCKET, PROTECTION);
            readBucketBufferAddresses[fdIndex] = addr;
            readBucketBuffers[fdIndex] = m.newDirectByteBuffer(addr, KEY_BUCKET_ONE_COST_SIZE);

            var addrRepl = pageManager.allocatePages(READ_BUCKET_FOR_REPL_BATCH_NUMBER, PROTECTION);
            readBucketBufferAddressesForRepl[fdIndex] = addrRepl;
            readBucketBuffersForRepl[fdIndex] = m.newDirectByteBuffer(addrRepl, READ_BUCKET_FOR_REPL_BATCH_NUMBER * KEY_BUCKET_ONE_COST_SIZE);

            writeBucketBufferAddressesArray[fdIndex] = new long[WRITE_BUCKET_BUFFER_GROUP_NUMBER];
            writeBucketBuffersArray[fdIndex] = new ByteBuffer[WRITE_BUCKET_BUFFER_GROUP_NUMBER];

            for (int j = 0; j < WRITE_BUCKET_BUFFER_GROUP_NUMBER; j++) {
                var addrWrite = pageManager.allocatePages(PAGE_NUMBER_PER_BUCKET, PROTECTION);
                writeBucketBufferAddressesArray[fdIndex][j] = addrWrite;
                writeBucketBuffersArray[fdIndex][j] = m.newDirectByteBuffer(addrWrite, KEY_BUCKET_ONE_COST_SIZE);
            }
        }
        log.info("Persist key bucket files fd opened, split number: {}, slot: {}", splitNumber, slot);
    }

    public void cleanUp() {
        if (fds != null) {
            for (int fd : fds) {
                if (fd == 0) {
                    continue;
                }

                int r = libC.close(fd);
                if (r < 0) {
                    System.err.println("Close fd error: " + libC.strerror(r));
                }
            }
            System.out.println("Closed fds, length: " + fds.length);
        }

        if (readBucketBufferAddresses != null) {
            for (var addr : readBucketBufferAddresses) {
                pageManager.freePages(addr, PAGE_NUMBER_PER_BUCKET);
            }
            System.out.println("Freed read segment buffer addresses, length: " + readBucketBufferAddresses.length);
        }

        if (readBucketBufferAddressesForRepl != null) {
            for (var addr : readBucketBufferAddressesForRepl) {
                pageManager.freePages(addr, READ_BUCKET_FOR_REPL_BATCH_NUMBER);
            }
            System.out.println("Freed read segment buffer addresses for repl, length: " + readBucketBufferAddressesForRepl.length);
        }

        if (writeBucketBufferAddressesArray != null) {
            for (var writeBucketBufferAddresses : writeBucketBufferAddressesArray) {
                if (writeBucketBufferAddresses == null) {
                    continue;
                }

                for (var addr : writeBucketBufferAddresses) {
                    if (addr != 0) {
                        pageManager.freePages(addr, PAGE_NUMBER_PER_BUCKET);
                    }
                }
            }
            System.out.println("Freed write segment buffer addresses, length: " + writeBucketBufferAddressesArray.length);
        }

        if (readPersistedKeyBucketCache != null) {
            readPersistedKeyBucketCache.cleanUp();
            System.out.println("Cleaned up read persisted key bucket cache");
        }

        if (metaKeyBucketSplitNumber != null) {
            metaKeyBucketSplitNumber.cleanUp();
            System.out.println("Cleaned up bucket split number");
        }

        if (statKeyBucketLastUpdateCount != null) {
            statKeyBucketLastUpdateCount.cleanUp();
        }
    }

    private record KeyBucketCacheKey(int bucketIndex, byte splitIndex) {
        @Override
        public String toString() {
            return "KeyBucketCacheKey{" +
                    "bucketIndex=" + bucketIndex +
                    ", splitIndex=" + splitIndex +
                    '}';
        }
    }

    public byte[] readKeyBucketBytesBatchToSlaveExists(byte splitIndex, int beginBucketIndex) {
        if (ConfForSlot.global.pureMemory) {
            if (allInMemoryBytes[splitIndex] == null) {
                return new byte[0];
            }

            var returnBytes = new byte[READ_BUCKET_FOR_REPL_BATCH_NUMBER * KEY_BUCKET_ONE_COST_SIZE];
            var returnBuffer = ByteBuffer.wrap(returnBytes);
            for (int i = 0; i < READ_BUCKET_FOR_REPL_BATCH_NUMBER; i++) {
                var bytes = allInMemoryBytes[splitIndex][beginBucketIndex + i];
                if (bytes != null) {
                    // bytes are compressed, one int for bytes length
                    System.arraycopy(bytes, 0, returnBytes, i * KEY_BUCKET_ONE_COST_SIZE + 4, bytes.length);
                    returnBuffer.putInt(i * KEY_BUCKET_ONE_COST_SIZE, bytes.length);
                }
            }
            return returnBytes;
        }

        int fdIndex = splitIndex;
        int fd = fds[fdIndex];
        var readBucketBuffer = readBucketBuffers[fdIndex];
        var readBucketBufferForRepl = readBucketBuffersForRepl[fdIndex];

        var offset = beginBucketIndex * KEY_BUCKET_ONE_COST_SIZE;
        int readN = READ_BUCKET_FOR_REPL_BATCH_NUMBER * KEY_BUCKET_ONE_COST_SIZE;

        // use same lock
        synchronized (readBucketBuffer) {
            // read / write use same buffer
            synchronized (readBucketBufferForRepl) {
                readBucketBufferForRepl.clear();

                int n = libC.pread(fd, readBucketBufferForRepl, readN, offset);
                if (n < 0) {
                    throw new IllegalStateException("Read persisted key bucket error, fd: " + fd + ", offset: " + offset +
                            ", length: " + readN + ", last error: " + libC.strerror(n));
                }
                readBucketBufferForRepl.rewind();

                if (n == 0) {
                    return null;
                }
                if (n != readN) {
                    throw new IllegalStateException("Read persisted key bucket error, fd: " + fd + ", offset: " + offset +
                            ", length: " + readN + ", last error: " + libC.strerror(n));
                }

                var bytes = new byte[readN];
                readBucketBufferForRepl.get(bytes);
                return bytes;
            }
        }
    }

    // need lock all, todo, need optimize
    public synchronized void writeKeyBucketBytesBatchFromMaster(byte[] contentBytes) {
        var splitIndex = contentBytes[0];
//        var splitNumber = contentBytes[1];
        var beginBucketIndex = ByteBuffer.wrap(contentBytes, 2, 4).getInt();
        // left length may be 0
        var leftLength = contentBytes.length - 1 - 1 - 4;

        if (ConfForSlot.global.pureMemory) {
            if (allInMemoryBytes[splitIndex] == null) {
                allInMemoryBytes[splitIndex] = new byte[bucketsPerSlot][];
            }

            if (leftLength == 0) {
                for (int i = 0; i < READ_BUCKET_FOR_REPL_BATCH_NUMBER; i++) {
                    var bucketIndex = beginBucketIndex + i;
                    allInMemoryBytes[splitIndex][bucketIndex] = null;
                }
            } else {
                var bucketCount = leftLength / KEY_BUCKET_ONE_COST_SIZE;
                if (bucketCount != READ_BUCKET_FOR_REPL_BATCH_NUMBER) {
                    throw new IllegalStateException("Write pure memory key bucket from master error,  bucket count batch not match, slot: "
                            + slot + ", split index: " + splitIndex + ", begin bucket index: " + beginBucketIndex + ", bucket count: " + bucketCount);
                }

                var returnBuffer = ByteBuffer.wrap(contentBytes, 1 + 1 + 4, leftLength);

                for (int i = 0; i < bucketCount; i++) {
                    var bucketIndex = beginBucketIndex + i;
                    var bytesLength = returnBuffer.getInt(i * KEY_BUCKET_ONE_COST_SIZE);
                    if (bytesLength == 0) {
                        allInMemoryBytes[splitIndex][bucketIndex] = null;
                    } else {
                        var bytes = new byte[bytesLength];
                        returnBuffer.get(i * KEY_BUCKET_ONE_COST_SIZE + 4, bytes);
                        allInMemoryBytes[splitIndex][bucketIndex] = bytes;
                    }
                }
            }

            log.info("Write pure memory key bucket from master success, slot: {}, split index: {}, begin bucket index: {}",
                    slot, splitIndex, beginBucketIndex);
            return;
        }

        int fdIndex = splitIndex;
        int fd = fds[fdIndex];
        if (fd == 0) {
            initFdForSlot((byte) (splitIndex + 1));
            fd = fds[fdIndex];
        }

        var groupIndex = beginBucketIndex % WRITE_BUCKET_BUFFER_GROUP_NUMBER;
        var writeBucketBuffer = writeBucketBuffersArray[fdIndex][groupIndex];
        var readBucketBufferForRepl = readBucketBuffersForRepl[fdIndex];

        var offset = beginBucketIndex * KEY_BUCKET_ONE_COST_SIZE;
        int writeN = READ_BUCKET_FOR_REPL_BATCH_NUMBER * KEY_BUCKET_ONE_COST_SIZE;

        synchronized (writeBucketBuffer) {
            synchronized (readBucketBufferForRepl) {
                readBucketBufferForRepl.clear();

                if (leftLength == 0) {
                    // pwrite append 0
                    var bytes0 = new byte[writeN];
                    readBucketBufferForRepl.put(bytes0);
                } else {
                    // splitIndex byte + splitNumber byte + beginBucketIndex int
                    readBucketBufferForRepl.put(contentBytes, 1 + 1 + 4, contentBytes.length - 1 - 1 - 4);
                }
                readBucketBufferForRepl.rewind();

                long beginT = System.nanoTime();
                int n = libC.pwrite(fd, readBucketBufferForRepl, writeN, offset);
                long costT = System.nanoTime() - beginT;
                if (n != writeN) {
                    throw new IllegalStateException("Write persisted key bucket from master error, slot: " + slot + ", fd: " + fd + ", offset: " + offset +
                            ", length: " + writeN + ", last error: " + libC.strerror(n));
                }

                pwriteCostNanos += costT;
                pwriteCount++;

                log.info("Write persisted key bucket from master success, slot: {}, fd: {}, offset: {}, length: {}", slot, fd, offset, writeN);
            }
        }
    }

    private final Function<KeyBucketCacheKey, @PolyNull byte[]> fnLoadPersistedKeyBucketBytes = keyBucketCacheKey -> {
        int fdIndex = keyBucketCacheKey.splitIndex;
        int fd = fds[fdIndex];
        var readBucketBuffer = readBucketBuffers[fdIndex];

        int offset = keyBucketCacheKey.bucketIndex * KEY_BUCKET_ONE_COST_SIZE;
        synchronized (readBucketBuffer) {
            readBucketBuffer.clear();

            int n = libC.pread(fd, readBucketBuffer, KEY_BUCKET_ONE_COST_SIZE, offset);
            if (n < 0) {
                throw new IllegalStateException("Read persisted key bucket error, fd: " + fd + ", offset: " + offset +
                        ", length: " + KEY_BUCKET_ONE_COST_SIZE + ", last error: " + libC.strerror(n));
            }
            readBucketBuffer.rewind();

            if (n == 0) {
                return null;
            }
            if (n != KEY_BUCKET_ONE_COST_SIZE) {
                throw new IllegalStateException("Read persisted key bucket error, fd: " + fd + ", offset: " + offset +
                        ", length: " + KEY_BUCKET_ONE_COST_SIZE + ", last error: " + libC.strerror(n));
            }

            // memory copy
            // only need compressed bytes, so the lru cache size is smaller
            // seq long + size int + uncompressed length int + compressed length int
            // refer to KeyBucket AFTER_COMPRESS_PREPEND_LENGTH
            int compressedLength = readBucketBuffer.getInt(AFTER_COMPRESS_PREPEND_LENGTH - 4);
            if (compressedLength == 0) {
                // pwrite append 0
                return null;
            }

            var bytesCompressed = new byte[compressedLength + AFTER_COMPRESS_PREPEND_LENGTH];
            readBucketBuffer.get(bytesCompressed);
            return bytesCompressed;
        }
    };

    private KeyBucket getKeyBucketInner(int bucketIndex, long keyHash) {
        var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
        int splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(keyHash % splitNumber);

        KeyBucket keyBucket;
        if (ConfForSlot.global.pureMemory) {
            if (allInMemoryBytes[splitIndex] == null) {
                return null;
            }

            var bytes = allInMemoryBytes[splitIndex][bucketIndex];
            if (bytes == null) {
                return null;
            }

            keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
            keyBucket.initWithCompressStats(compressStats);
        } else {
            var keyBucketCacheKey = new KeyBucketCacheKey(bucketIndex, (byte) splitIndex);
            var bytes = readPersistedKeyBucketCache.get(keyBucketCacheKey, fnLoadPersistedKeyBucketBytes);
            if (bytes == null) {
                return null;
            }

            keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
            keyBucket.initWithCompressStats(compressStats);
        }
        return keyBucket;
    }

    public KeyBucket.ValueBytesWithExpireAt get(int bucketIndex, byte[] keyBytes, long keyHash) {
        var keyBucket = getKeyBucket(bucketIndex, keyHash);
        if (keyBucket == null) {
            return null;
        }
        return keyBucket.getValueByKey(keyBytes, keyHash);
    }

    private KeyBucket getKeyBucket(int bucketIndex, long keyHash) {
        var rl = rwlArray[bucketIndex].readLock();
        rl.lock();
        try {
            return getKeyBucketInner(bucketIndex, keyHash);
        } finally {
            rl.unlock();
        }
    }

    public ArrayList<KeyBucket> getKeyBuckets(int bucketIndex) {
        ArrayList<KeyBucket> keyBuckets;
        var rl = rwlArray[bucketIndex].readLock();
        rl.lock();
        try {
            var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
            keyBuckets = new ArrayList<>(splitNumber);

            for (int i = 0; i < splitNumber; i++) {
                var splitIndex = i;

                KeyBucket keyBucket;
                if (ConfForSlot.global.pureMemory) {
                    if (allInMemoryBytes[splitIndex] == null) {
                        keyBuckets.add(null);
                        continue;
                    }

                    var bytes = allInMemoryBytes[splitIndex][bucketIndex];
                    if (bytes == null) {
                        keyBuckets.add(null);
                        continue;
                    }

                    keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
                } else {
                    var keyBucketCacheKey = new KeyBucketCacheKey(bucketIndex, (byte) splitIndex);
                    var bytes = readPersistedKeyBucketCache.get(keyBucketCacheKey, fnLoadPersistedKeyBucketBytes);
                    if (bytes == null) {
                        keyBuckets.add(null);
                        continue;
                    }

                    keyBucket = new KeyBucket(slot, bucketIndex, (byte) i, splitNumber, bytes, snowFlake);
                }

                keyBucket.initWithCompressStats(compressStats);
                keyBuckets.add(keyBucket);
            }
        } finally {
            rl.unlock();
        }
        return keyBuckets;
    }

    public synchronized void updateKeyBucketFromMasterNewly(int bucketIndex, byte splitIndex, byte splitNumber, long lastUpdateSeq, byte[] bytes) {
        var wl = rwlArray[bucketIndex].writeLock();
        wl.lock();
        try {
            updateKeyBucketInner(bucketIndex, splitIndex, splitNumber, lastUpdateSeq, bytes);
        } finally {
            wl.unlock();
        }
    }

    // already in write lock, reentrant
    private synchronized void updateKeyBucketInner(int bucketIndex, byte splitIndex, byte splitNumber, long lastUpdateSeq, byte[] bytes) {
        if (bytes.length > KEY_BUCKET_ONE_COST_SIZE) {
            throw new IllegalStateException("Key bucket bytes size too large, slot: " + slot +
                    ", bucket index: " + bucketIndex + ", split index: " + splitIndex + ", size: " + bytes.length);
        }

        if (ConfForSlot.global.pureMemory) {
            if (allInMemoryBytes[splitIndex] == null) {
                allInMemoryBytes[splitIndex] = new byte[bucketsPerSlot][];
            }
            allInMemoryBytes[splitIndex][bucketIndex] = bytes;
        } else {
            var keyBucketCacheKey = new KeyBucketCacheKey(bucketIndex, splitIndex);
            readPersistedKeyBucketCache.put(keyBucketCacheKey, bytes);

            // persist
            int fdIndex = splitIndex;
            int fd = fds[fdIndex];
            if (fd == 0) {
                // split number already changed
                initFdForSlot(splitNumber);
                fd = fds[fdIndex];
            }

            var offset = bucketIndex * KEY_BUCKET_ONE_COST_SIZE;
            var groupIndex = bucketIndex % WRITE_BUCKET_BUFFER_GROUP_NUMBER;
            var writeBucketBuffer = writeBucketBuffersArray[fdIndex][groupIndex];
            synchronized (writeBucketBuffer) {
                writeBucketBuffer.clear();
                writeBucketBuffer.put(bytes);
                writeBucketBuffer.rewind();

                long beginT = System.nanoTime();
                int n = libC.pwrite(fd, writeBucketBuffer, KEY_BUCKET_ONE_COST_SIZE, offset);
                long costT = System.nanoTime() - beginT;
                if (n != KEY_BUCKET_ONE_COST_SIZE) {
                    throw new IllegalStateException("Write persisted key bucket error, slot: " + slot + ", fd: " + fd + ", offset: " + offset +
                            ", length: " + KEY_BUCKET_ONE_COST_SIZE + ", last error: " + libC.strerror(n));
                }

                pwriteCostNanos += costT;
                pwriteCount++;
            }
        }

        if (masterUpdateCallback != null) {
            masterUpdateCallback.onKeyBucketUpdate(slot, bucketIndex, splitIndex, splitNumber, lastUpdateSeq, bytes);
        }
    }

    private void updateKeyBucketInner(int bucketIndex, KeyBucket keyBucket) {
        updateKeyBucketInner(bucketIndex, keyBucket.splitIndex, keyBucket.splitNumber, keyBucket.lastUpdateSeq, keyBucket.compress());
    }

    public interface BucketLockCallback {
        void call();
    }

    public synchronized void bucketLock(int bucketIndex, BucketLockCallback callback) {
        var wl = rwlArray[bucketIndex].writeLock();
        wl.lock();
        try {
            callback.call();
        } finally {
            wl.unlock();
        }
    }

    interface UpdateBatchCallback {
        void call(final ArrayList<KeyBucket> keyBuckets, final boolean[] putFlags, final byte splitNumber, final boolean isLoadedAll);
    }

    private long updateBatchCount = 0;

    synchronized void updateBatch(int bucketIndex, long keyHash, UpdateBatchCallback callback) {
        var wl = rwlArray[bucketIndex].writeLock();
        wl.lock();
        try {
            var splitNumber = metaKeyBucketSplitNumber.get(bucketIndex);
            ArrayList<KeyBucket> keyBuckets = new ArrayList<>(splitNumber);

            var isSingleKeyUpdate = keyHash != 0;
            // just get one key bucket
            if (isSingleKeyUpdate) {
                var splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(keyHash % splitNumber);
                byte[] bytes;
                if (ConfForSlot.global.pureMemory) {
                    if (allInMemoryBytes[splitIndex] == null) {
                        bytes = null;
                    } else {
                        bytes = allInMemoryBytes[splitIndex][bucketIndex];
                    }
                } else {
                    var keyBucketCacheKey = new KeyBucketCacheKey(bucketIndex, (byte) splitIndex);
                    bytes = readPersistedKeyBucketCache.get(keyBucketCacheKey, fnLoadPersistedKeyBucketBytes);
                }

                // bytes can be null
                var keyBucket = new KeyBucket(slot, bucketIndex, (byte) splitIndex, splitNumber, bytes, snowFlake);
                keyBucket.initWithCompressStats(compressStats);
                keyBuckets.add(keyBucket);
            } else {
                for (int i = 0; i < splitNumber; i++) {
                    var splitIndex = i;
                    byte[] bytes;
                    if (ConfForSlot.global.pureMemory) {
                        if (allInMemoryBytes[splitIndex] == null) {
                            bytes = null;
                        } else {
                            bytes = allInMemoryBytes[splitIndex][bucketIndex];
                        }
                    } else {
                        var keyBucketCacheKey = new KeyBucketCacheKey(bucketIndex, (byte) splitIndex);
                        bytes = readPersistedKeyBucketCache.get(keyBucketCacheKey, fnLoadPersistedKeyBucketBytes);
                    }

                    // bytes can be null
                    var keyBucket = new KeyBucket(slot, bucketIndex, (byte) i, splitNumber, bytes, snowFlake);
                    keyBucket.initWithCompressStats(compressStats);
                    keyBuckets.add(keyBucket);
                }
            }

            boolean[] putBackFlags = new boolean[splitNumber];
            callback.call(keyBuckets, putBackFlags, splitNumber, keyHash == 0);

            boolean sizeChanged = false;
            for (int i = 0; i < splitNumber; i++) {
                if (putBackFlags[i]) {
                    var keyBucket = keyBuckets.get(i);
                    updateKeyBucketInner(bucketIndex, keyBucket);
                    sizeChanged = true;
                }
            }

            // key count for each key bucket, is not accurate
            if (sizeChanged && !isSingleKeyUpdate) {
                int keyCount = 0;
                for (var keyBucket : keyBuckets) {
                    if (keyBucket != null) {
                        keyCount += keyBucket.size;
                    }
                }

                updateBatchCount++;
                boolean isSync = updateBatchCount % 10 == 0;
                // can be async for better performance, but key count is not accurate
                statKeyBucketLastUpdateCount.setKeyCountInBucketIndex(bucketIndex, (short) keyCount, isSync);
            }
        } finally {
            wl.unlock();
        }
    }

    private record PvmRow(long keyHash, long expireAt, byte[] keyBytes, byte[] encoded) {
        @Override
        public String toString() {
            return "PvmRow{" +
                    "key=" + new String(keyBytes) +
                    ", keyHash=" + keyHash +
                    ", expireAt=" + expireAt +
                    '}';
        }
    }

    public void updatePvmListAfterWriteSegment(ArrayList<PersistValueMeta> pvmList) {
        var groupByBucketIndex = pvmList.stream().collect(Collectors.groupingBy(one -> one.bucketIndex));
        for (var entry : groupByBucketIndex.entrySet()) {
            var bucketIndex = entry.getKey();
            var list = entry.getValue();

            ArrayList<PvmRow> pvmRowList = new ArrayList<>(list.size());
            for (var pvm : list) {
                pvmRowList.add(new PvmRow(pvm.keyHash, pvm.expireAt, pvm.keyBytes, pvm.encode()));
            }
            persistPvmListBatch(bucketIndex, pvmRowList);
        }
    }

    private void persistPvmListBatch(int bucketIndex, ArrayList<PvmRow> pvmRowList) {
        updateBatch(bucketIndex, 0, (keyBuckets, putBackFlags, splitNumber, isLoadedAll) -> {
            var beforeKeyBuckets = new ArrayList<>(keyBuckets);
            byte[] beforeSplitNumberArr = new byte[]{splitNumber};

            for (int i = 0; i < pvmRowList.size(); i++) {
                var pvmRow = pvmRowList.get(i);
                int splitIndex = beforeSplitNumberArr[0] == 1 ? 0 : (int) Math.abs(pvmRow.keyHash() % beforeSplitNumberArr[0]);
                var keyBucket = beforeKeyBuckets.get(splitIndex);

                boolean notSplit = beforeSplitNumberArr[0] == splitNumber;
                var afterPutKeyBuckets = notSplit ? new KeyBucket[SPLIT_MULTI_STEP] : null;

                double loadFactor = keyBucket.loadFactor();
                if (loadFactor > KeyBucket.HIGH_LOAD_FACTOR && afterPutKeyBuckets == null) {
                    // stop this batch, do next time
                    var leftPvmRowList = new ArrayList<PvmRow>(pvmRowList.size() - i);
                    leftPvmRowList.addAll(pvmRowList.subList(i, pvmRowList.size()));
                    persistPvmListBatch(bucketIndex, leftPvmRowList);
                    break;
                }

                boolean isPut = keyBucket.put(pvmRow.keyBytes(), pvmRow.keyHash(), pvmRow.expireAt(), pvmRow.encoded(), afterPutKeyBuckets);
                if (!isPut) {
                    throw new IllegalStateException("Put pvm error, pvm: " + pvmRow);
                }

                splitOthersIfSplit(bucketIndex, keyBuckets, putBackFlags,
                        beforeKeyBuckets, beforeSplitNumberArr, splitIndex, keyBucket, notSplit, afterPutKeyBuckets);
            }
        });
    }

    public void persistShortValueListBatch(int bucketIndex, List<Wal.V> shortValueList) {
        updateBatch(bucketIndex, 0, (keyBuckets, putBackFlags, splitNumber, isLoadedAll) -> {
            var beforeKeyBuckets = new ArrayList<>(keyBuckets);
            byte[] beforeSplitNumberArr = new byte[]{splitNumber};

            for (int i = 0; i < shortValueList.size(); i++) {
                var v = shortValueList.get(i);
                int splitIndex = beforeSplitNumberArr[0] == 1 ? 0 : (int) Math.abs(v.keyHash() % beforeSplitNumberArr[0]);
                var keyBucket = beforeKeyBuckets.get(splitIndex);

                boolean notSplit = beforeSplitNumberArr[0] == splitNumber;
                var afterPutKeyBuckets = notSplit ? new KeyBucket[SPLIT_MULTI_STEP] : null;

                // delete must not split
                if (v.isDeleteFlag()) {
                    if (keyBucket.del(v.key().getBytes(), v.keyHash()) && notSplit) {
                        putBackFlags[splitIndex] = true;
                    }
                    continue;
                }

                double loadFactor = keyBucket.loadFactor();
                if (loadFactor > KeyBucket.HIGH_LOAD_FACTOR && afterPutKeyBuckets == null) {
                    // stop this batch, do next time
                    // why ? I forget why I write this... sign, need test, todo
                    var leftShortValueList = new ArrayList<Wal.V>(shortValueList.size() - i);
                    leftShortValueList.addAll(shortValueList.subList(i, shortValueList.size()));
                    persistShortValueListBatch(bucketIndex, leftShortValueList);
                    break;
                }

                boolean isPut = keyBucket.put(v.key().getBytes(), v.keyHash(), v.expireAt(), v.cvEncoded(), afterPutKeyBuckets);
                if (!isPut) {
                    throw new IllegalStateException("Put short value error, key: " + v.key());
                }

                splitOthersIfSplit(bucketIndex, keyBuckets, putBackFlags,
                        beforeKeyBuckets, beforeSplitNumberArr,
                        splitIndex, keyBucket, notSplit, afterPutKeyBuckets);
            }
        });
    }

    private void splitOthersIfSplit(int bucketIndex, ArrayList<KeyBucket> keyBuckets, boolean[] putBackFlags,
                                    ArrayList<KeyBucket> beforeKeyBuckets, byte[] beforeSplitNumberArr,
                                    int splitIndex, KeyBucket keyBucket, boolean notSplit, KeyBucket[] afterPutKeyBuckets) {
        if (notSplit) {
            putBackFlags[splitIndex] = true;
        }

        if (afterPutKeyBuckets != null && afterPutKeyBuckets[0] != null) {
            // in write lock, stats need not thread safe here
            splitCount++;
            splitCostNanos += afterPutKeyBuckets[0].lastSplitCostNanos;

            // save all these
            for (var afterPutKeyBucket : afterPutKeyBuckets) {
                updateKeyBucketInner(bucketIndex, afterPutKeyBucket);
            }
            // already saved
            putBackFlags[splitIndex] = false;

            beforeKeyBuckets.clear();
            beforeKeyBuckets.addAll(Arrays.asList(afterPutKeyBuckets));

            // other key bucket also need split
            for (var kb : keyBuckets) {
                if (kb == keyBucket) {
                    continue;
                }

                var kbArray = kb.split();
                for (var bucket : kbArray) {
                    updateKeyBucketInner(bucketIndex, bucket);
                    beforeKeyBuckets.add(bucket);
                }
            }

            if (beforeKeyBuckets.size() != keyBuckets.size() * SPLIT_MULTI_STEP) {
                throw new IllegalStateException("After split key bucket size not match, size: " + beforeKeyBuckets.size() +
                        ", before size: " + keyBuckets.size() + ", split multi step: " + SPLIT_MULTI_STEP);
            }

            // sort by split index
            beforeKeyBuckets.sort(Comparator.comparingInt(kb -> kb.splitIndex));

            if (afterPutKeyBuckets[0].splitNumber == beforeSplitNumberArr[0]) {
                throw new IllegalStateException("Split number not changed after split, split number: "
                        + afterPutKeyBuckets[0].splitNumber);
            }
            beforeSplitNumberArr[0] = afterPutKeyBuckets[0].splitNumber;

            // update meta
            metaKeyBucketSplitNumber.set(bucketIndex, beforeSplitNumberArr[0]);
            masterUpdateCallback.onKeyBucketSplit(slot, bucketIndex, beforeSplitNumberArr[0]);
        }
    }

    public boolean remove(int bucketIndex, byte[] keyBytes, long keyHash) {
        boolean[] deleteFlags = new boolean[1];
        updateBatch(bucketIndex, keyHash, (keyBuckets, putFlags, splitNumber, isLoadedAll) -> {
            // key masked value is not 0, just get one target key bucket
            var keyBucket = keyBuckets.get(0);
            if (keyBucket.size == 0) {
                return;
            }

            boolean isDel = keyBucket.del(keyBytes, keyHash);
            if (isDel) {
                putFlags[0] = true;
            }
            deleteFlags[0] = isDel;
        });
        return deleteFlags[0];
    }

    public synchronized void flush() {
        metaKeyBucketSplitNumber.clear();
        statKeyBucketLastUpdateCount.clear();

        boolean[] ftruncateFlags = new boolean[MAX_SPLIT_NUMBER];

        var wl = this.rwlArray[0].writeLock();
        wl.lock();
        try {
            for (int i = 0; i < bucketsPerSlot; i++) {
                for (int fdIndex = 0; fdIndex < MAX_SPLIT_NUMBER; fdIndex++) {
                    var keyBucketCacheKey = new KeyBucketCacheKey((short) i, (byte) fdIndex);
                    readPersistedKeyBucketCache.invalidate(keyBucketCacheKey);

                    var fd = fds[fdIndex];
                    if (fd == 0) {
                        continue;
                    }

                    if (ftruncateFlags[fdIndex]) {
                        continue;
                    }

                    // truncate
                    int r = libC.ftruncate(fd, 0);
                    if (r < 0) {
                        log.error("Truncate persisted key bucket file error, fd: {}, slot: {}, split: {}, last error: {}",
                                fd, slot, fdIndex, libC.strerror(r));
                    }
                    log.warn("Truncate persisted key bucket file, fd: {}, slot: {}, split: {}", fd, slot, fdIndex);
                    ftruncateFlags[fdIndex] = true;
                }
            }
        } finally {
            wl.unlock();
        }
    }

    @Override
    public List<StatKV> stats() {
        var list = new ArrayList<StatKV>();
        final String prefix = "key loader s-" + slot + " ";

        list.add(new StatKV(prefix + "pwrite count", pwriteCount));
        // pwrite cost avg
        if (pwriteCount > 0) {
            list.add(new StatKV(prefix + "pwrite cost avg micros", (double) pwriteCostNanos / pwriteCount / 1000));
        }

        list.add(StatKV.split);
        // cost too much time
        list.add(new StatKV(prefix + "bucket count", bucketsPerSlot));
        list.add(new StatKV(prefix + "persist key size", getKeyCount()));

        // todo, not correct
        list.add(new StatKV(prefix + "loaded key size", compressStats.getAllTmpBucketSize()));
        list.add(new StatKV(prefix + "hot key size in 1 hour", compressStats.getAllLruBucketSize()));
        list.add(new StatKV(prefix + "hot bucket size in 1 hour", compressStats.getLruBucketSize()));

        list.add(StatKV.split);
        list.add(new StatKV(prefix + "split count", splitCount));
        if (splitCount > 0) {
            list.add(new StatKV(prefix + "split cost avg micros", (double) splitCostNanos / splitCount / 1000));
        }

        var stats = readPersistedKeyBucketCache.stats();
        final String prefix2 = "key bucket cache s-" + slot + " ";
        list.add(StatKV.split);
        OfStats.cacheStatsToList(list, stats, prefix2);

        list.add(StatKV.split);
        list.addAll(compressStats.stats());

        return list;
    }
}
