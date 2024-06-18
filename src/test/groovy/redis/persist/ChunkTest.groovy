package redis.persist

import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class ChunkTest extends Specification {
    def 'test segment index'() {
        given:
        final byte slot = 0
        def snowFlake = new SnowFlake(1, 1)

        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, 0)

        def confChunk = ConfForSlot.global.confChunk
        confChunk.fdPerChunk = 2
        confChunk.segmentNumberPerFd = 4096

        var chunk = new Chunk(slot, Consts.slotDir, oneSlot, snowFlake, null, null)
        oneSlot.chunk = chunk

        when:
        def segmentNumberPerFd = confChunk.segmentNumberPerFd
        int halfSegmentNumber = confChunk.maxSegmentNumber() / 2

        then:
        chunk.targetFdIndex(0) == 0
        chunk.targetFdIndex(segmentNumberPerFd - 1) == 0
        chunk.targetFdIndex(segmentNumberPerFd) == 1
        chunk.targetFdIndex(segmentNumberPerFd * 2 - 1) == 1

        chunk.targetSegmentIndexTargetFd(0) == 0
        chunk.targetSegmentIndexTargetFd(segmentNumberPerFd - 1) == segmentNumberPerFd - 1
        chunk.targetSegmentIndexTargetFd(segmentNumberPerFd) == 0
        chunk.targetSegmentIndexTargetFd(segmentNumberPerFd * 2 - 1) == segmentNumberPerFd - 1

        chunk.initSegmentIndexWhenFirstStart(0)

        when:
        chunk.moveSegmentIndexNext(1)

        then:
        chunk.segmentIndex == 1

        when:
        chunk.moveSegmentIndexNext(segmentNumberPerFd - 1)

        then:
        chunk.segmentIndex == segmentNumberPerFd

        when:
        chunk.moveSegmentIndexNext(segmentNumberPerFd - 1)

        then:
        chunk.segmentIndex == 0

        when:
        boolean isMoveOverflow = false
        try {
            chunk.moveSegmentIndexNext(confChunk.maxSegmentNumber() + 1)
        } catch (SegmentOverflowException e) {
            isMoveOverflow = true
        }

        then:
        isMoveOverflow

        when:
        chunk.segmentIndex = confChunk.maxSegmentNumber() - 1
        chunk.moveSegmentIndexNext(1)

        then:
        chunk.segmentIndex == 0

        when:
        List<Integer> needMergeSegmentIndexListNewAppend = []
        List<Integer> needMergeSegmentIndexListNotNewAppend = []
        confChunk.maxSegmentNumber().times {
            needMergeSegmentIndexListNewAppend << chunk.needMergeSegmentIndex(true, it)
            needMergeSegmentIndexListNotNewAppend << chunk.needMergeSegmentIndex(false, it)
        }

        println needMergeSegmentIndexListNewAppend
        println needMergeSegmentIndexListNotNewAppend

        then:
        chunk.needMergeSegmentIndex(true, halfSegmentNumber) == 0
        chunk.needMergeSegmentIndex(true, halfSegmentNumber + 10) == 10
        chunk.needMergeSegmentIndex(true, halfSegmentNumber - 10) == -1
        chunk.needMergeSegmentIndex(false, halfSegmentNumber - 10) == halfSegmentNumber * 2 - 10

        needMergeSegmentIndexListNewAppend.count { it == -1 } == halfSegmentNumber
        new HashSet(needMergeSegmentIndexListNewAppend.findAll { it != -1 }).size() == halfSegmentNumber
        new HashSet(needMergeSegmentIndexListNotNewAppend).size() == confChunk.maxSegmentNumber()

        needMergeSegmentIndexListNewAppend[halfSegmentNumber] == 0
        needMergeSegmentIndexListNewAppend[0] == -1
        needMergeSegmentIndexListNewAppend[-1] == halfSegmentNumber - 1

        needMergeSegmentIndexListNotNewAppend[halfSegmentNumber] == 0
        needMergeSegmentIndexListNotNewAppend[0] == halfSegmentNumber
        needMergeSegmentIndexListNotNewAppend[halfSegmentNumber - 1] == confChunk.maxSegmentNumber() - 1
        needMergeSegmentIndexListNotNewAppend[-1] == halfSegmentNumber - 1

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test reuse segments'() {
        given:
        final byte slot = 0
        def snowFlake = new SnowFlake(1, 1)

        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, 0)

        def confChunk = ConfForSlot.global.confChunk
        confChunk.fdPerChunk = 2
        confChunk.segmentNumberPerFd = 4096

        var chunk = new Chunk(slot, Consts.slotDir, oneSlot, snowFlake, null, null)
        oneSlot.chunk = chunk

        when:
        chunk.segmentIndex = 0
        chunk.reuseSegments(true, 1, true)

        then:
        oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(0).flag == Chunk.SEGMENT_FLAG_REUSE
        !chunk.reuseSegments(true, 1, true)
        chunk.reuseSegments(false, 1, true)

        when:
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_NEW, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(1, Chunk.SEGMENT_FLAG_MERGING, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(2, Chunk.SEGMENT_FLAG_MERGED, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(3, Chunk.SEGMENT_FLAG_INIT, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(4, Chunk.SEGMENT_FLAG_INIT, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(5, Chunk.SEGMENT_FLAG_INIT, 1L, 0)

        then:
        chunk.reuseSegments(false, 3, false)
        chunk.segmentIndex == 3

        when:
        chunk.segmentIndex = 0
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(2, Chunk.SEGMENT_FLAG_INIT, 1L, 0)
        chunk.reuseSegments(false, 3, false)

        then:
        chunk.segmentIndex == 2

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }
}
