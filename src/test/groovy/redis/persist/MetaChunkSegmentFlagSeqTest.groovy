package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Chunk.NO_NEED_MERGE_SEGMENT_INDEX
import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentFlagSeqTest extends Specification {
    def 'test for repl'() {
        given:
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        def allInMemoryCachedBytes = one.getInMemoryCachedBytes()

        then:
        allInMemoryCachedBytes.length == one.allCapacity

        when:
        ConfForSlot.global.pureMemory = false

        def bytes0 = new byte[one.allCapacity]
        one.overwriteInMemoryCachedBytes(bytes0)

        then:
        one.inMemoryCachedBytes.length == one.allCapacity

        when:
        ConfForSlot.global.pureMemory = true

        one.overwriteInMemoryCachedBytes(bytes0)

        then:
        one.inMemoryCachedBytes.length == one.allCapacity

        when:
        boolean exception = false
        def bytes0WrongSize = new byte[one.allCapacity - 1]

        try {
            one.overwriteInMemoryCachedBytes(bytes0WrongSize)
        } catch (e) {
            exception = true
        }

        then:
        exception

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForSlot.global.pureMemory = false
        slotDir.deleteDir()
    }

    def 'test read write seq'() {
        given:
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        ConfForSlot.global.pureMemory = false

        one.setSegmentMergeFlag(10, (byte) 1, 1L, 0)
        def segmentFlag = one.getSegmentMergeFlag(10)

        then:
        segmentFlag.flag == 1
        segmentFlag.segmentSeq == 1L
        segmentFlag.walGroupIndex == 0

        when:
        ConfForSlot.global.pureMemory = true

        one.setSegmentMergeFlag(10, (byte) 1, 1L, 0)
        segmentFlag = one.getSegmentMergeFlag(10)

        then:
        segmentFlag.flag == 1
        segmentFlag.segmentSeq == 1L
        segmentFlag.walGroupIndex == 0

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForSlot.global.pureMemory = false
    }

    def 'test read write seq pure memory'() {
        given:
        ConfForSlot.global.pureMemory = true

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        one.setSegmentMergeFlag(10, (byte) 1, 1L, 0)
        one.setSegmentMergeFlag(11, (byte) 2, 2L, 11)
        def segmentFlag = one.getSegmentMergeFlag(10)
        def segmentFlagList = one.getSegmentMergeFlagBatch(10, 2)

        then:
        segmentFlag.flag == 1
        segmentFlag.segmentSeq == 1L
        segmentFlag.walGroupIndex == 0

        segmentFlagList.size() == 2
        segmentFlagList[0].flag == 1
        segmentFlagList[0].segmentSeq == 1L
        segmentFlagList[0].walGroupIndex == 0
        segmentFlagList[1].flag == 2
        segmentFlagList[1].segmentSeq == 2L
        segmentFlagList[1].walGroupIndex == 11

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForSlot.global.pureMemory = false
    }

    def 'test read batch for repl'() {
        given:
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        ConfForSlot.global.pureMemory = false

        List<Long> seqLongList = []
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, (byte) 1, seqLongList, 0)

        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        when:
        ConfForSlot.global.pureMemory = true

        seqLongList.clear()
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, (byte) 1, seqLongList, 0)

        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        cleanup:
        one.clear()
        one.cleanUp()
        ConfForSlot.global.pureMemory = false
    }

    def 'test iterate'() {
        given:
        def confChunk = ConfForSlot.global.confChunk
        def targetConfChunk = ConfForSlot.ConfChunk.c1m
//        def targetConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = targetConfChunk.segmentNumberPerFd
        confChunk.fdPerChunk = targetConfChunk.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        new File('chunk_segment_flag.txt').withWriter { writer ->
            one.iterateAll { segmentIndex, flag, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flag, $seq, $walGroupIndex")
            }
        }

        new File('chunk_segment_flag_range.txt').withWriter { writer ->
            one.iterateRange(1024, 1024) { segmentIndex, flag, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flag, $seq, $walGroupIndex")
            }
        }

        then:
        1 == 1

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'test iterate and find'() {
        given:
        ConfForSlot.global.pureMemory = true

        def confChunk = ConfForSlot.global.confChunk
        def targetConfChunk = ConfForSlot.ConfChunk.c100m
//        def targetConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = targetConfChunk.segmentNumberPerFd
        confChunk.fdPerChunk = targetConfChunk.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        int targetWalGroupIndex = 1

        and:
        final byte slot = 0
        var chunk = new Chunk(slot, Consts.slotDir, null, null, null, null)

        when:
        def r = one.iterateAndFind(1024, 1024 * 10, targetWalGroupIndex, chunk)

        then:
        r[0] == NO_NEED_MERGE_SEGMENT_INDEX
        r[1] == 0

        when:
        one.setSegmentMergeFlag(1024, Chunk.SEGMENT_FLAG_NEW, 1L, targetWalGroupIndex)
        def r2 = one.iterateAndFind(1024, 1024 * 10, targetWalGroupIndex, chunk)

        then:
        r2[0] == 1024
        r2[1] == FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT

        when:
        one.setSegmentMergeFlag(1024, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, targetWalGroupIndex)
        r2 = one.iterateAndFind(1024, 1024 * 10, targetWalGroupIndex, chunk)

        then:
        r2[0] == 1024
        r2[1] == FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT

        when:
        one.setSegmentMergeFlag(confChunk.segmentNumberPerFd - 1, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, targetWalGroupIndex)
        one.setSegmentMergeFlag(confChunk.segmentNumberPerFd, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, targetWalGroupIndex)
        r2 = one.iterateAndFind(confChunk.segmentNumberPerFd - 1, 1024 * 10, targetWalGroupIndex, chunk)

        then:
        r2[0] == confChunk.segmentNumberPerFd - 1
        r2[1] == 1

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'test get merged segment index'() {
        given:
        ConfForSlot.global.pureMemory = true

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        // all init
        def i = one.getMergedSegmentIndexEndLastTime(0, 0)

        then:
        i == NO_NEED_MERGE_SEGMENT_INDEX

        when:
        var maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber()
        int halfSegmentNumber = maxSegmentNumber / 2

        one.setSegmentMergeFlag(10, Chunk.SEGMENT_FLAG_NEW, 1L, 0)
        one.setSegmentMergeFlag(11, Chunk.SEGMENT_FLAG_NEW, 1L, 0)
        def i2 = one.getMergedSegmentIndexEndLastTime(halfSegmentNumber, halfSegmentNumber)

        then:
        i2 == 9

        when:
        one.setSegmentMergeFlag(10, Chunk.SEGMENT_FLAG_MERGED, 1L, 0)
        i2 = one.getMergedSegmentIndexEndLastTime(halfSegmentNumber, halfSegmentNumber)

        then:
        i2 == 10

        when:
        one.setSegmentMergeFlag(10, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, 1L, 0)
        i2 = one.getMergedSegmentIndexEndLastTime(halfSegmentNumber, halfSegmentNumber)

        then:
        i2 == 10

        when:
        def i3 = one.getMergedSegmentIndexEndLastTime(0, halfSegmentNumber)

        then:
        i3 == maxSegmentNumber - 1

        cleanup:
        ConfForSlot.global.pureMemory = false
    }
}
