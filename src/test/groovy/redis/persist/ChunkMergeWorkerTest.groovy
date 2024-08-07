package redis.persist

import redis.Debug
import spock.lang.Specification

class ChunkMergeWorkerTest extends Specification {
    final byte slot = 0
    final byte slotNumber = 1

    def 'test base'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker
        chunkMergeWorker.resetThreshold(Wal.calcWalGroupNumber())
        chunkMergeWorker.MERGED_CV_SIZE_THRESHOLD = 1000

        ChunkMergeWorker.innerGauge.collect()
        chunkMergeWorker.mergedSegmentCount = 1
        ChunkMergeWorker.innerGauge.collect()

        def ms0 = new ChunkMergeWorker.MergedSegment(0, 1)
        def ms1 = new ChunkMergeWorker.MergedSegment(1, 1)
        println ms0
        println ms1
        expect:
        ms0 < ms1

        when:
        int walGroupIndex = 0
        int bucketIndex = 0
        int segmentIndex = 0
        then:
        chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex) == null

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, 32, 1))
        chunkMergeWorker.addMergedSegment(0, 1)
        then:
        chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex) == null

        when:
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, bucketIndex, segmentIndex))
        chunkMergeWorker.addMergedSegment(segmentIndex, 1)
        def r = chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(walGroupIndex)
        then:
        r.segmentIndexList().size() == 1
        r.vList().size() == 1

        when:
        chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal([0], 0)
        Debug.instance.logMerge = true
        chunkMergeWorker.logMergeCount = 999
        chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal([1], 1)
        then:
        chunkMergeWorker.mergedCvList.size() == 0
        chunkMergeWorker.mergedSegmentSet.size() == 0

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test persist'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunkMergeWorker = oneSlot.chunkMergeWorker
        chunkMergeWorker.MERGED_CV_SIZE_THRESHOLD = 1000

        and:
        int walGroupIndex = 0
        int bucketIndex = 0
        int segmentIndex = 0

        when:
        def cvList = Mock.prepareCompressedValueList(10)
        for (cv in cvList) {
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, bucketIndex, segmentIndex))
        }
        chunkMergeWorker.addMergedSegment(segmentIndex, cvList.size())
        Debug.instance.logMerge = true
        chunkMergeWorker.logMergeCount = 999
        chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk()
        chunkMergeWorker.persistAllMergedCvListInTargetSegmentIndexList([segmentIndex])
        then:
        chunkMergeWorker.mergedCvList.size() == 0
        chunkMergeWorker.mergedSegmentSet.size() == 0

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
