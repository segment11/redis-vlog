package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class ChunkTest extends Specification {
    static Chunk prepareOne(byte slot) {
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)

        def confChunk = ConfForSlot.global.confChunk
        confChunk.fdPerChunk = 2
        confChunk.segmentNumberPerFd = 4096

        def snowFlake = new SnowFlake(1, 1)
        var chunk = new Chunk(slot, Consts.slotDir, oneSlot, snowFlake, null, null)
        oneSlot.chunk = chunk

        chunk
    }

    def 'test base'() {
        given:
        def flagInit = Chunk.Flag.init
        println flagInit
        println new Chunk.SegmentFlag(flagInit, 1L, 0)

        expect:
        Chunk.Flag.init.canReuse() && Chunk.Flag.reuse.canReuse() && Chunk.Flag.merged_and_persisted.canReuse()
        Chunk.Flag.merging.isMergingOrMerged() && Chunk.Flag.merged.isMergingOrMerged()

        when:
        byte flagByte = (byte) -200
        boolean exception = false
        try {
            Chunk.Flag.fromFlagByte(flagByte)
        } catch (IllegalArgumentException ignore) {
            exception = true
        }
        then:
        exception
    }

    def 'test segment index'() {
        given:
        final byte slot = 0
        def chunk = prepareOne(slot)
        def oneSlot = chunk.oneSlot
        def confChunk = ConfForSlot.global.confChunk

        when:
        Chunk.chunkPersistGauge.collect()
        def segmentNumberPerFd = confChunk.segmentNumberPerFd
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()
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
        chunk.initSegmentIndexWhenFirstStart(chunk.maxSegmentIndex + 1)

        when:
        chunk.moveSegmentIndexNext(1)
        then:
        chunk.segmentIndex == 1
        chunk.currentSegmentIndex() == 1
        chunk.targetFdIndex() == 0
        chunk.targetSegmentIndexTargetFd() == 1

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
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test reuse segments'() {
        given:
        final byte slot = 0
        def chunk = prepareOne(slot)
        def oneSlot = chunk.oneSlot

        when:
        chunk.segmentIndex = 0
        chunk.reuseSegments(true, 1, true)
        then:
        oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(0).flag() == Chunk.Flag.reuse
        chunk.reuseSegments(true, 1, true)
        chunk.reuseSegments(false, 1, true)

        when:
        chunk.segmentIndex = 0
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.Flag.merged_and_persisted, 1L, 0)
        then:
        chunk.reuseSegments(false, 1, false)
        chunk.reuseSegments(false, 1, true)

        when:
        chunk.segmentIndex = 0
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.Flag.new_write, 1L, 0)
        then:
        !chunk.reuseSegments(false, 1, false)

        when:
        chunk.segmentIndex = 0
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.Flag.new_write, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(1, Chunk.Flag.merging, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(2, Chunk.Flag.merged, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(3, Chunk.Flag.init, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(4, Chunk.Flag.init, 1L, 0)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(5, Chunk.Flag.init, 1L, 0)
        then:
        chunk.reuseSegments(false, 3, false)
        chunk.segmentIndex == 3

        when:
        chunk.segmentIndex = 0
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(2, Chunk.Flag.init, 1L, 0)
        chunk.reuseSegments(false, 3, false)
        then:
        chunk.segmentIndex == 2

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test pread'() {
        given:
        final byte slot = 0
        def chunk = prepareOne(slot)
        def oneSlot = chunk.oneSlot

        and:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        chunk.initFds(libC)

        when:
        def bytes = chunk.preadForMerge(0, 10)
        then:
        bytes == null

        when:
        boolean exception = false
        try {
            bytes = chunk.preadForMerge(0, FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT + 1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        when:
        bytes = chunk.preadForRepl(0)
        then:
        bytes == null

        when:
        bytes = chunk.preadOneSegment(0)
        then:
        bytes == null

        cleanup:
        chunk.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }
}
