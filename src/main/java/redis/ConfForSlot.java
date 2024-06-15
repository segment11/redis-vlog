package redis;

import redis.persist.Chunk;
import redis.persist.KeyBucket;
import redis.persist.Wal;

import static redis.persist.LocalPersist.PAGE_SIZE;

public enum ConfForSlot {
    debugMode(100_000), c1m(1_000_000L),
    c10m(10_000_000L), c100m(100_000_000L);

    public long estimateKeyNumber;
    public int estimateOneValueLength = DEFAULT_ESTIMATE_ONE_VALUE_LENGTH;
    private static final int DEFAULT_ESTIMATE_ONE_VALUE_LENGTH = 200;
    static final int MAX_ONE_VALUE_LENGTH = 200;

    public String netListenAddresses;

    public final ConfBucket confBucket;
    public final ConfChunk confChunk;
    public final ConfWal confWal;
    public final ConfLru lruBigString = new ConfLru(1000);
    public final ConfLru lruKeyAndCompressedValueEncoded = new ConfLru(1_00_000);

    public boolean pureMemory = false;
    public short slotNumber = 1;
    public byte netWorkers = 1;
    public int eventLoopIdleMillis = 10;

    public static ConfForSlot from(long estimateKeyNumber) {
        if (estimateKeyNumber <= 100_000L) {
            return debugMode;
        } else if (estimateKeyNumber <= 1_000_000L) {
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

        if (estimateKeyNumber == 100_000L) {
            this.confChunk = ConfChunk.debugMode;
            this.confBucket = ConfBucket.debugMode;
            this.confWal = ConfWal.debugMode;
        } else if (estimateKeyNumber == 1_000_000L) {
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
                ", estimateOneValueLength=" + estimateOneValueLength +
                ", confChunk=" + confChunk +
                ", confBucket=" + confBucket +
                ", confWal=" + confWal +
                '}';
    }

    public static class ConfLru {
        public ConfLru(int maxSize) {
            this.maxSize = maxSize;
        }

        public int maxSize;
    }

    public enum ConfBucket {
        debugMode(4096, (byte) 1),
        c1m(KeyBucket.DEFAULT_BUCKETS_PER_SLOT, (byte) 1),
        c10m(KeyBucket.MAX_BUCKETS_PER_SLOT, (byte) 1),
        c100m(KeyBucket.MAX_BUCKETS_PER_SLOT, (byte) 9);

        ConfBucket(int bucketsPerSlot, byte initialSplitNumber) {
            this.bucketsPerSlot = bucketsPerSlot;
            this.initialSplitNumber = initialSplitNumber;
        }

        public int bucketsPerSlot;
        public byte initialSplitNumber;

        // 4KB one segment, 25 * 1000 * 4KB = 100MB
        public ConfLru lruPerFd = new ConfLru(0);

        @Override
        public String toString() {
            return "ConfBucket{" +
                    "bucketsPerSlot=" + bucketsPerSlot +
                    '}';
        }
    }

    public enum ConfChunk {
        debugMode(4 * 1024, (byte) 2, PAGE_SIZE),
        c1m(64 * 1024, (byte) 2, PAGE_SIZE),
        c10m(512 * 1024, (byte) 1, PAGE_SIZE),
        c100m(512 * 1024, (byte) 8, PAGE_SIZE);

        ConfChunk(int segmentNumberPerFd, byte fdPerChunk, int segmentLength) {
            this.segmentNumberPerFd = segmentNumberPerFd;
            this.fdPerChunk = fdPerChunk;
            this.segmentLength = segmentLength;
        }

        public static final int MAX_FD_PER_CHUNK = 64;

        // each slot each worker persist to a file, one file one chunk, each file max 2GB, 4KB page size, each file max 512K pages
        public int segmentNumberPerFd;
        // 16 * 2GB = 32GB per slot (per worker)
        // suppose one key value encoded length (value is already compressed) ~= 100 byte, one page size 4096 contains 40 key value pairs
        // one fd contains 512K pages, so one fd contains 20M key value pairs
        // one chunk contains 20M * 16 = 320M key value pairs
        // merge worker is another chunk, so one slot may contain 640M key value pairs

        // if one key value encoded length (value is already compressed) ~= 500 byte, one page size 4096 contains 8 key value pairs
        // fd per chunk need to be 32 or 64
        public byte fdPerChunk;
        // for better latency, PAGE_SIZE 4K is ok
        public int segmentLength;

        // 4KB one segment, 25 * 1000 * 4KB = 100MB
        public ConfLru lruPerFd = new ConfLru(0);

        public int maxSegmentNumber() {
            return segmentNumberPerFd * fdPerChunk;
        }

        public void resetByOneValueLength(int estimateOneValueLength) {
            if (estimateOneValueLength <= 200) {
                return;
            }

            if (estimateOneValueLength <= 500) {
                this.fdPerChunk = (byte) (2 * this.fdPerChunk);
                return;
            }

            if (estimateOneValueLength <= 1000) {
                this.segmentNumberPerFd = this.segmentNumberPerFd / 4;
                this.segmentLength = PAGE_SIZE * 4;
                this.fdPerChunk = (byte) (4 * this.fdPerChunk);

                Chunk.ONCE_PREPARE_SEGMENT_COUNT = 16;
                Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE = 4;
                return;
            }

            if (estimateOneValueLength <= MAX_ONE_VALUE_LENGTH) {
                this.segmentNumberPerFd = this.segmentNumberPerFd / 4;
                this.segmentLength = PAGE_SIZE * 4;
                this.fdPerChunk = (byte) (8 * this.fdPerChunk);

                Chunk.ONCE_PREPARE_SEGMENT_COUNT = 16;
                Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE = 4;
                return;
            }

            throw new IllegalArgumentException("Estimate one value length too large: " + estimateOneValueLength +
                    ", should be less than " + MAX_ONE_VALUE_LENGTH);
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
        debugMode(32, 1000, 1000),
        c1m(32, 1000, 1000),
        c10m(32, 1000, 1000),
        c100m(32, 1000, 1000);

        ConfWal(int oneChargeBucketNumber, int valueSizeTrigger, int shortValueSizeTrigger) {
            this.oneChargeBucketNumber = oneChargeBucketNumber;
            this.valueSizeTrigger = valueSizeTrigger;
            this.shortValueSizeTrigger = shortValueSizeTrigger;
        }

        public int oneChargeBucketNumber;

        // refer to Chunk BATCH_SEGMENT_COUNT_FOR_PWRITE
        // 4 pages ~= 16KB, one V persist length is about 100B, so 4 pages can store about 160 V
        // for better latency, do not configure too large
        // 200 make sure there is at least one batch 16KB
        public int valueSizeTrigger;
        public int shortValueSizeTrigger;

        private void resetWalStaticValues() {
            if (oneChargeBucketNumber != 32) {
                Wal.ONE_GROUP_BUFFER_SIZE = PAGE_SIZE * oneChargeBucketNumber;
                Wal.EMPTY_BYTES_FOR_ONE_GROUP = new byte[Wal.ONE_GROUP_BUFFER_SIZE];
                Wal.GROUP_COUNT_IN_M4 = 4 * 1024 * 1024 / Wal.ONE_GROUP_BUFFER_SIZE;
            }
            Wal.doLogAfterInit();
        }

        public void resetByOneValueLength(int estimateOneValueLength) {
            if (estimateOneValueLength <= 200) {
                resetWalStaticValues();
                return;
            }

            if (estimateOneValueLength <= 500) {
                this.valueSizeTrigger = 500;
                resetWalStaticValues();
                return;
            }

            if (estimateOneValueLength <= 1000) {
                this.valueSizeTrigger = 100;
                this.oneChargeBucketNumber = 16;
                resetWalStaticValues();
                return;
            }

            if (estimateOneValueLength <= MAX_ONE_VALUE_LENGTH) {
                this.valueSizeTrigger = 50;
                this.oneChargeBucketNumber = 16;
                resetWalStaticValues();
                return;
            }

            throw new IllegalArgumentException("Estimate one value length too large: " + estimateOneValueLength +
                    ", should be less than " + MAX_ONE_VALUE_LENGTH);
        }

        @Override
        public String toString() {
            return "ConfWal{" +
                    "oneChargeBucketNumber=" + oneChargeBucketNumber +
                    ", valueSizeTrigger=" + valueSizeTrigger +
                    ", shortValueSizeTrigger=" + shortValueSizeTrigger +
                    '}';
        }
    }
}
