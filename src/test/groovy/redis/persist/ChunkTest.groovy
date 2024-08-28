package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForGlobal
import redis.ConfForSlot
import redis.SnowFlake
import redis.repl.incremental.XOneWalGroupPersist
import spock.lang.Specification

import static redis.persist.FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD

class ChunkTest extends Specification {
    static Chunk prepareOne(short slot, boolean withKeyLoader = false) {
        def confChunk = ConfForSlot.global.confChunk
        confChunk.fdPerChunk = 2
        confChunk.segmentNumberPerFd = 4096

        def keyLoader = withKeyLoader ? KeyLoaderTest.prepareKeyLoader() : null
        def snowFlake = keyLoader ? keyLoader.snowFlake : new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, null)

        def chunk = new Chunk(slot, Consts.slotDir, oneSlot, snowFlake, keyLoader)
        oneSlot.chunk = chunk

        chunk
    }

    final short slot = 0

    def 'test base'() {
        given:
        def flagInit = Chunk.Flag.init
        println flagInit
        println flagInit.flagByte()
        println new Chunk.SegmentFlag(flagInit, 1L, 0)

        expect:
        Chunk.Flag.init.canReuse() && Chunk.Flag.reuse.canReuse() && Chunk.Flag.merged_and_persisted.canReuse()
        Chunk.Flag.merging.isMergingOrMerged() && Chunk.Flag.merged.isMergingOrMerged()
        !Chunk.Flag.merging.canReuse()
        !Chunk.Flag.init.isMergingOrMerged()

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
        def chunk = prepareOne(slot)
        def oneSlot = chunk.oneSlot
        def confChunk = ConfForSlot.global.confChunk

        println chunk

        when:
        chunk.persistCountTotal = 1
        chunk.persistCvCountTotal = 100
        chunk.updatePvmBatchCostTimeTotalUs = 100
        Chunk.chunkGauge.collect()
        def segmentNumberPerFd = confChunk.segmentNumberPerFd
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()
        chunk.cleanUp()
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
            println e.message
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

        when:
        chunk.mergedSegmentIndexEndLastTime = Chunk.NO_NEED_MERGE_SEGMENT_INDEX
        chunk.mergedSegmentIndexEndLastTimeAfterSlaveCatchUp = Chunk.NO_NEED_MERGE_SEGMENT_INDEX
        chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart()
        chunk.mergedSegmentIndexEndLastTime = 0
        chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart()
        chunk.mergedSegmentIndexEndLastTime = chunk.maxSegmentIndex - 1
        chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart()
        boolean exception = false
        try {
            chunk.mergedSegmentIndexEndLastTime = chunk.maxSegmentIndex
            chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart()
        } catch (IllegalStateException ignored) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            chunk.mergedSegmentIndexEndLastTime = -100
            chunk.checkMergedSegmentIndexEndLastTimeValidAfterServerStart()
        } catch (IllegalStateException ignored) {
            exception = true
        }
        then:
        exception

        when:
        chunk.segmentIndex = halfSegmentNumber - 1
        chunk.moveSegmentIndexNext(1)
        then:
        chunk.segmentIndex == halfSegmentNumber

        when:
        chunk.resetAsFlush()
        then:
        chunk.segmentIndex == 0
        chunk.mergedSegmentIndexEndLastTime == Chunk.NO_NEED_MERGE_SEGMENT_INDEX

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test reuse segments'() {
        given:
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
        chunk.reuseSegments(false, 1, false)
        chunk.segmentIndex == 1

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

        when:
        chunk.segmentIndex = 0
        5.times {
            oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0 + it, Chunk.Flag.new_write, 1L, 0)
        }
        then:
        !chunk.reuseSegments(true, 5, false)

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
    }

    def 'test pread'() {
        given:
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
            chunk.preadForMerge(0, FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE + 1)
        } catch (IllegalArgumentException e) {
            println e.message
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

    def 'test persist'() {
        given:
        def chunk = prepareOne(slot, true)
        def oneSlot = chunk.oneSlot
        def confChunk = ConfForSlot.global.confChunk

        and:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        chunk.initFds(libC)

        def xForBinlog = new XOneWalGroupPersist(true, false, 0)

        when:
        def vList = Mock.prepareValueList(100)
        chunk.segmentIndex = 0
        def r = chunk.persist(0, vList, false, xForBinlog)
        then:
        chunk.segmentIndex == 1
        r.size() == 0

        when:
        List<Long> blankSeqList = []
        4.times {
            blankSeqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqList.size(), Chunk.Flag.init, blankSeqList, 0)
        chunk.segmentIndex = 0
        r = chunk.persist(0, vList, true, xForBinlog)
        then:
        chunk.segmentIndex == 1
        r.size() == 0

//        when:
//        chunk.segmentIndex = confChunk.maxSegmentNumber() - 10
//        boolean exception = false
//        try {
//            chunk.persist(0, vList, false, xForBinlog)
//        } catch (SegmentOverflowException ignore) {
//            exception = true
//        }
//        then:
//        exception

//        when:
//        exception = false
//        try {
//            chunk.persist(0, vList, true, xForBinlog)
//        } catch (SegmentOverflowException ignore) {
//            exception = true
//        }
//        then:
//        exception

        when:
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqList.size(), Chunk.Flag.init, blankSeqList, 0)
        chunk.segmentIndex = 0
        def vListManyCount = Mock.prepareValueList(100, 0)
        (1..<16).each {
            vListManyCount.addAll Mock.prepareValueList(100, it)
        }
        chunk.persist(0, vListManyCount, false, xForBinlog)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE

        when:
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()
        chunk.fdLengths[0] = 4096 * 100
        chunk.mergedSegmentIndexEndLastTime = halfSegmentNumber - 1
        List<Long> seqList = []
        Chunk.ONCE_PREPARE_SEGMENT_COUNT.times {
            seqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, seqList.size(), Chunk.Flag.merged_and_persisted, seqList, 0)
        chunk.segmentIndex = 0
        r = chunk.persist(0, vListManyCount, false, xForBinlog)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE
        r.size() == chunk.segmentIndex

        when:
        chunk.fdLengths[0] = 0
        chunk.mergedSegmentIndexEndLastTime = Chunk.NO_NEED_MERGE_SEGMENT_INDEX
        chunk.segmentIndex = halfSegmentNumber
        r = chunk.persist(0, vList, false, xForBinlog)
        then:
        r.size() > 0

        when:
        ConfForGlobal.pureMemory = true
        chunk.fdReadWriteArray[0].initByteBuffers(true)
        println 'mock pure memory chunk append segments bytes, fd: ' + chunk.fdReadWriteArray[0].name
        for (frw in oneSlot.keyLoader.fdReadWriteArray) {
            if (frw != null) {
                frw.initByteBuffers(false)
                println 'mock pure memory key loader set key buckets bytes, fd: ' + frw.name
            }
        }
        // begin persist
        List<Long> blankSeqListMany = []
        32.times {
            blankSeqList << 0L
        }
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, blankSeqListMany.size(), Chunk.Flag.init, blankSeqListMany, 0)
        chunk.segmentIndex = 0
        chunk.persist(0, vListManyCount, false, xForBinlog)
        then:
        chunk.segmentIndex > FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE

        when:
        chunk.writeSegmentToTargetSegmentIndex(new byte[4096], 0)
        then:
        chunk.segmentIndex == 0

        when:
        chunk.clearOneSegmentForPureMemoryModeAfterMergedAndPersisted(0)
        then:
        chunk.preadOneSegment(0) == null

        cleanup:
        ConfForGlobal.pureMemory = false
        chunk.cleanUp()
        oneSlot.keyLoader.flush()
        oneSlot.keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test repl'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk

        when:
        def replBytes = new byte[4096]
        chunk.writeSegmentsFromMasterExists(replBytes, 0, 1)
        then:
        1 == 1

        when:
        // write again, fd length will not change
        chunk.writeSegmentsFromMasterExists(replBytes, 0, 1)
        then:
        1 == 1

        when:
        boolean exception = false
        try {
            chunk.writeSegmentsFromMasterExists(replBytes, 0, REPL_ONCE_SEGMENT_COUNT_PREAD + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        ConfForGlobal.pureMemory = true
        for (fdReadWrite in chunk.fdReadWriteArray) {
            fdReadWrite.initByteBuffers(true)
        }
        chunk.writeSegmentsFromMasterExists(replBytes, 0, 1)
        then:
        1 == 1

        when:
        def replBytes2 = new byte[4096 * 2]
        chunk.writeSegmentsFromMasterExists(replBytes2, 0, 2)
        then:
        1 == 1

        cleanup:
        ConfForGlobal.pureMemory = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test need merge segment index list'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk
        println 'in memory size estimate: ' + chunk.estimate()

        def confChunk = ConfForSlot.global.confChunk
        int halfSegmentNumber = (confChunk.maxSegmentNumber() / 2).intValue()

        when:
        boolean exception = false
        ArrayList<Integer> needMergeSegmentIndexList = [1, chunk.maxSegmentIndex]
        try {
            chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        needMergeSegmentIndexList = [0, 1, chunk.maxSegmentIndex]
        try {
            chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        chunk.mergedSegmentIndexEndLastTime = chunk.maxSegmentIndex - 2
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == 1

        when:
        exception = false
        chunk.mergedSegmentIndexEndLastTime = Chunk.NO_NEED_MERGE_SEGMENT_INDEX
        needMergeSegmentIndexList = [1]
        try {
            chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        chunk.mergedSegmentIndexEndLastTime = Chunk.NO_NEED_MERGE_SEGMENT_INDEX
        needMergeSegmentIndexList = [0, 1]
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == 1

        when:
        chunk.mergedSegmentIndexEndLastTime = 1
        needMergeSegmentIndexList = [3]
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == 3

        when:
        chunk.mergedSegmentIndexEndLastTime = chunk.maxSegmentIndex - 10
        needMergeSegmentIndexList = [chunk.maxSegmentIndex - 2, chunk.maxSegmentIndex - 1]
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == chunk.maxSegmentIndex

        when:
        chunk.mergedSegmentIndexEndLastTime = halfSegmentNumber - 20
        needMergeSegmentIndexList = [halfSegmentNumber - 10, halfSegmentNumber - 9]
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == halfSegmentNumber - 1

        when:
        chunk.mergedSegmentIndexEndLastTime = halfSegmentNumber - 100
        needMergeSegmentIndexList = [halfSegmentNumber - 90, halfSegmentNumber - 89]
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == halfSegmentNumber - 89

        when:
        chunk.mergedSegmentIndexEndLastTime = halfSegmentNumber + 100
        needMergeSegmentIndexList = [halfSegmentNumber + 101, halfSegmentNumber + 102]
        chunk.updateLastMergedSegmentIndexEnd(needMergeSegmentIndexList)
        then:
        chunk.mergedSegmentIndexEndLastTime == halfSegmentNumber + 102

        when:
        TreeSet<Integer> sorted = [0]
        chunk.checkNeedMergeSegmentIndexListContinuous(sorted)
        then:
        1 == 1

        when:
        exception = false
        sorted = [0, 2]
        try {
            chunk.checkNeedMergeSegmentIndexListContinuous(sorted)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        sorted.clear()
        (Chunk.ONCE_PREPARE_SEGMENT_COUNT * 4 + 1).times {
            sorted << it
        }
        try {
            chunk.checkNeedMergeSegmentIndexListContinuous(sorted)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        sorted.clear()
        (Chunk.ONCE_PREPARE_SEGMENT_COUNT * 4).times {
            sorted << it
        }
        chunk.checkNeedMergeSegmentIndexListContinuous(sorted)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
