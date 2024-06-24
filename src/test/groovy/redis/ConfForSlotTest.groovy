package redis

import redis.persist.Chunk
import redis.persist.Wal
import spock.lang.Specification

class ConfForSlotTest extends Specification {
    def 'test all'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)
        def c = ConfForSlot.global

        expect:
        c.estimateKeyNumber == 1_000_000
        c.estimateOneValueLength == 200

        c.isValueSetUseCompression
        c.isOnDynTrainDictForCompression

        c.netListenAddresses == null
        c.lruBigString.maxSize == 1000
        c.lruKeyAndCompressedValueEncoded.maxSize == 100_000
        !c.pureMemory
        c.slotNumber == 1
        c.netWorkers == 1
        c.eventLoopIdleMillis == 10
        println c

        c.confBucket.bucketsPerSlot == 16384
        c.confBucket.initialSplitNumber == 3
        c.confBucket.lruPerFd.maxSize == 0
        println c.confBucket

        c.confChunk.segmentNumberPerFd == 256 * 1024
        c.confChunk.fdPerChunk == 1
        c.confChunk.maxSegmentNumber() == 256 * 1024
        c.confChunk.segmentLength == 4096
        c.confChunk.fdPerChunk < ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK
        c.confChunk.lruPerFd.maxSize == 0
        println c.confChunk

        c.confWal.oneChargeBucketNumber == 32
        c.confWal.valueSizeTrigger == 1000
        c.confWal.shortValueSizeTrigger == 1000
        println c.confWal
    }

    def 'test different estimate key number'() {
        given:
        def c100k = ConfForSlot.from(100_000)
        def c1m = ConfForSlot.from(1_000_000)
        def c10m = ConfForSlot.from(10_000_000)
        def c100m = ConfForSlot.from(100_000_000)

        expect:
        c100k == ConfForSlot.debugMode
        c1m == ConfForSlot.c1m
        c10m == ConfForSlot.c10m
        c100m == ConfForSlot.c100m
    }

    def 'test chunk reset by one value length'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)
        def c = ConfForSlot.global

        c.confChunk.mark();

        when:
        ConfForSlot.global.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(200)

        then:
        c.confChunk.fdPerChunk == 1
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 8

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(200)

        then:
        c.confChunk.fdPerChunk == 2
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 16

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(500)

        then:
        c.confChunk.fdPerChunk == 2
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 8

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(500)

        then:
        c.confChunk.fdPerChunk == 4
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 16

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(1000)

        then:
        c.confChunk.segmentNumberPerFd == 256 * 1024 / 4
        c.confChunk.segmentLength == 4096 * 4
        c.confChunk.fdPerChunk == 4
        Chunk.ONCE_PREPARE_SEGMENT_COUNT == 32
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 4

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(1000)

        then:
        c.confChunk.segmentNumberPerFd == 256 * 1024 / 4
        c.confChunk.segmentLength == 4096 * 4
        c.confChunk.fdPerChunk == 8
        Chunk.ONCE_PREPARE_SEGMENT_COUNT == 32
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 8

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(2000)

        then:
        c.confChunk.segmentNumberPerFd == 256 * 1024 / 4
        c.confChunk.segmentLength == 4096 * 4
        c.confChunk.fdPerChunk == 8
        Chunk.ONCE_PREPARE_SEGMENT_COUNT == 32
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 4

        when:
        c.confChunk.reset();
        ConfForSlot.global.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(2000)

        then:
        c.confChunk.segmentNumberPerFd == 256 * 1024 / 4
        c.confChunk.segmentLength == 4096 * 4
        c.confChunk.fdPerChunk == 16
        Chunk.ONCE_PREPARE_SEGMENT_COUNT == 32
        Chunk.ONCE_PREPARE_SEGMENT_COUNT_FOR_MERGE == 8

        when:
        c.confChunk.reset();
        boolean exception = false
        try {
            c.confChunk.resetByOneValueLength(4000)
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception
    }

    def 'test wal reset by one value length'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)
        def c = ConfForSlot.global

        c.confWal.mark()

        when:
        c.confWal.resetByOneValueLength(200)

        then:
        c.confWal.valueSizeTrigger == 1000
        Wal.ONE_GROUP_BUFFER_SIZE == 4096 * 32 / 4

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(500)

        then:
        c.confWal.valueSizeTrigger == 500
        Wal.ONE_GROUP_BUFFER_SIZE == 4096 * 32 / 2

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(1000)

        then:
        c.confWal.oneChargeBucketNumber == 16
        c.confWal.valueSizeTrigger == 100
        Wal.ONE_GROUP_BUFFER_SIZE == 4096 * 16 / 1

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(2000)

        then:
        c.confWal.oneChargeBucketNumber == 16
        c.confWal.valueSizeTrigger == 50
        Wal.ONE_GROUP_BUFFER_SIZE == 4096 * 16 / 1

        when:
        c.confWal.reset()
        boolean exception = false

        try {
            c.confWal.resetByOneValueLength(4000)
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception
    }
}
