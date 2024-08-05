package redis.persist

import redis.BaseCommand
import redis.CompressedValue
import spock.lang.Specification

class ChunkMergeJobTest extends Specification {
    final byte slot = 0
    final byte slotNumber = 1

    def 'merge segments'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker

        def cv = new CompressedValue()
        def cvWithKeyAndSegmentOffset = new ChunkMergeJob.CvWithKeyAndSegmentOffset(cv, 'key', 0, 0, (byte) 0)
        println cvWithKeyAndSegmentOffset.shortString()
        println cvWithKeyAndSegmentOffset

        and:
        int segmentIndex = 0
        ArrayList<Integer> needMergeSegmentIndexList = []
        10.times {
            needMergeSegmentIndexList << (segmentIndex + it)
        }
        oneSlot.setSegmentMergeFlag(segmentIndex, Chunk.Flag.new_write, 1L, 0)
        oneSlot.setSegmentMergeFlag(segmentIndex + 1, Chunk.Flag.init, 1L, 0)
        def job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, oneSlot.snowFlake)

        when:
        // chunk segments not write yet, in wal
        OneSlotTest.batchPut(oneSlot, 300)
        job.mergeSegments(needMergeSegmentIndexList)
        then:
        1 == 1

        when:
        def bucketIndex0KeyList = OneSlotTest.batchPut(oneSlot, 300, 100, 0, slotNumber)
        def bucketIndex1KeyList = OneSlotTest.batchPut(oneSlot, 300, 100, 1, slotNumber)
        def testRemovedByWalKey0 = bucketIndex0KeyList[0]
        def testUpdatedByWalKey1 = bucketIndex0KeyList[1]
        def testRemovedByWalKey2 = bucketIndex0KeyList[2]
        def testUpdatedByKeyLoaderKey3 = bucketIndex0KeyList[3]
        def sTest0 = BaseCommand.slot(testRemovedByWalKey0.bytes, slotNumber)
        def sTest1 = BaseCommand.slot(testUpdatedByWalKey1.bytes, slotNumber)
        def sTest2 = BaseCommand.slot(testRemovedByWalKey2.bytes, slotNumber)
        def sTest3 = BaseCommand.slot(testUpdatedByKeyLoaderKey3.bytes, slotNumber)

        oneSlot.removeDelay(testRemovedByWalKey0, 0, sTest0.keyHash())
        oneSlot.keyLoader.removeSingleKeyForTest(0, testRemovedByWalKey2.bytes, sTest2.keyHash())
        oneSlot.keyLoader.putValueByKeyForTest(0, testUpdatedByKeyLoaderKey3.bytes, sTest3.keyHash(), 0, 0L, new byte[10])
        def cv2 = new CompressedValue()
        cv2.keyHash = sTest1.keyHash()
        cv2.seq = oneSlot.snowFlake.nextId()
        oneSlot.put(testUpdatedByWalKey1, 0, cv2)
        job.mergeSegments(needMergeSegmentIndexList)
        then:
        1 == 1

        when:
        10.times {
            OneSlotTest.batchPut(oneSlot, 100, 100, 0, slotNumber)
        }
        List<Long> seqList = []
        10.times {
            seqList << 1L
        }
        oneSlot.setSegmentMergeFlagBatch(segmentIndex, 10, Chunk.Flag.new_write, seqList, 0)
        job.mergeSegments(needMergeSegmentIndexList)
        then:
        1 == 1

        when:
        boolean exception = false
        needMergeSegmentIndexList[-1] = segmentIndex + 2
        try {
            job.mergeSegments(needMergeSegmentIndexList)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def needMergeSegmentIndexList2 = [0, 1, oneSlot.chunk.maxSegmentIndex - 1, oneSlot.chunk.maxSegmentIndex]
        def job2 = new ChunkMergeJob(slot, needMergeSegmentIndexList2, chunkMergeWorker, oneSlot.snowFlake)
        job2.run()
        then:
        1 == 1

        when:
        needMergeSegmentIndexList2[0] = 1
        exception = false
        try {
            job2.run()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
