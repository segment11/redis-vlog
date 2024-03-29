package redis;

import redis.persist.KeyBucket;

import static redis.persist.LocalPersist.PAGE_SIZE;

public enum ConfForSlot {
    c1m(1_000_000L), c10m(10_000_000L), c100m(100_000_000L);

    public long estimateKeyNumber;
    public String netListenAddresses;

    public final ConfBucket confBucket;
    public final ConfChunk confChunk;
    public final ConfWal confWal;

    public static ConfForSlot from(long estimateKeyNumber) {
        if (estimateKeyNumber <= 1_000_000L) {
            return c1m;
        } else if (estimateKeyNumber <= 10_000_000L) {
            return c10m;
        } else {
            return c100m;
        }
    }

    public static ConfForSlot global = c1m;

    ConfForSlot(long estimateKeyNumber) {
        this.estimateKeyNumber = estimateKeyNumber;

        if (estimateKeyNumber == 1_000_000L) {
            this.confChunk = ConfChunk.c1m;
            this.confBucket = ConfBucket.c1m;
            this.confWal = ConfWal.c1m;
        } else if (estimateKeyNumber == 10_000_000L) {
            this.confChunk = ConfChunk.c10m;
            this.confBucket = ConfBucket.c10m;
            this.confWal = ConfWal.c10m;
        } else {
            this.confChunk = ConfChunk.c100m;
            this.confBucket = ConfBucket.c100m;
            this.confWal = ConfWal.c100m;
        }
    }

    @Override
    public String toString() {
        return "ConfForSlot{" +
                "estimateKeyNumber=" + estimateKeyNumber +
                ", confChunk=" + confChunk +
                ", confBucket=" + confBucket +
                ", confWal=" + confWal +
                '}';
    }

    public static class ConfLru {
        public ConfLru(long expireAfterWrite, long expireAfterAccess, long maximumBytes) {
            this.expireAfterWrite = expireAfterWrite;
            this.expireAfterAccess = expireAfterAccess;
            this.maximumBytes = maximumBytes;
        }

        public long expireAfterWrite;
        public long expireAfterAccess;
        public long maximumBytes;
    }

    public enum ConfBucket {
        debugMode(KeyBucket.DEFAULT_BUCKETS_PER_SLOT), c1m(KeyBucket.DEFAULT_BUCKETS_PER_SLOT), c10m(65536), c100m(KeyBucket.MAX_BUCKETS_PER_SLOT);

        ConfBucket(int bucketsPerSlot) {
            this.bucketsPerSlot = bucketsPerSlot;
        }

        public int bucketsPerSlot;

        public ConfLru lru = new ConfLru(300, 300, 100_000_000L);

        @Override
        public String toString() {
            return "ConfBucket{" +
                    "bucketsPerSlot=" + bucketsPerSlot +
                    '}';
        }
    }

    public enum ConfChunk {
        debugMode(4 * 1024, (byte) 1, PAGE_SIZE),
        c1m(64 * 1024, (byte) 2, PAGE_SIZE),
        c10m(256 * 1024, (byte) 4, PAGE_SIZE),
        c100m(512 * 1024, (byte) 8, PAGE_SIZE);

        ConfChunk(int segmentNumberPerFd, byte fdPerChunk, int segmentLength) {
            this.segmentNumberPerFd = segmentNumberPerFd;
            this.fdPerChunk = fdPerChunk;
            this.segmentLength = segmentLength;
        }

        // each slot each worker persist to a file, one file one chunk, each file max 2GB, 4KB page size, each file max 512K pages
        public int segmentNumberPerFd;
        // 16 * 2GB = 32GB per slot (per worker)
        // suppose one key value encoded length ~= 100 byte, one page size 4096 contains 40 key value pairs
        // one fd contains 512K pages, so one fd contains 20M key value pairs
        // one chunk contains 20M * 16 = 320M key value pairs
        // merge worker is another chunk, so one slot may contain 640M key value pairs
        public byte fdPerChunk;
        // for better latency, PAGE_SIZE 4K is ok
        public int segmentLength;

        public ConfLru lru = new ConfLru(300, 300, 100_000_000L);

        public int maxSegmentNumber() {
            return segmentNumberPerFd * fdPerChunk;
        }

        public int maxSegmentIndex() {
            return maxSegmentNumber() - 1;
        }

        @Override
        public String toString() {
            return "ConfChunk{" +
                    "segmentNumberPerFd=" + segmentNumberPerFd +
                    ", fdPerChunk=" + fdPerChunk +
                    ", segmentLength=" + segmentLength +
                    '}';
        }
    }

    public enum ConfWal {
        debugMode(16, 2, 1000, 1000),
        c1m(16, 2, 1000, 1000),
        c10m(32, 2, 1000, 1000),
        c100m(32, 2, 1000, 1000);

        ConfWal(int oneChargeBucketNumber, int batchNumber, int valueSizeTrigger, int shortValueSizeTrigger) {
            this.oneChargeBucketNumber = oneChargeBucketNumber;
            this.batchNumber = batchNumber;
            this.valueSizeTrigger = valueSizeTrigger;
            this.shortValueSizeTrigger = shortValueSizeTrigger;
        }

        public int oneChargeBucketNumber;
        public int batchNumber;

        // refer to Chunk BATCH_SEGMENT_COUNT_FOR_PWRITE
        // 4 pages ~= 16KB, one V persist length is about 100B, so 4 pages can store about 160 V
        // for better latency, do not configure too large
        // 200 make sure there is at least one batch 16KB
        public int valueSizeTrigger;
        public int shortValueSizeTrigger;

        @Override
        public String toString() {
            return "ConfWal{" +
                    "oneChargeBucketNumber=" + oneChargeBucketNumber +
                    ", batchNumber=" + batchNumber +
                    ", valueSizeTrigger=" + valueSizeTrigger +
                    ", shortValueSizeTrigger=" + shortValueSizeTrigger +
                    '}';
        }
    }
}
