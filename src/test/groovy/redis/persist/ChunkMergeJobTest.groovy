package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class ChunkMergeJobTest extends Specification {
    def 'merge segments'() {
        given:
        final byte slot = 0
        final int bucketIndex = 0
        final int segmentIndex = 0
        final int walGroupIndex = 0
        def snowFlake = new SnowFlake(1, 1)

        and:
        var list = Mock.prepareValueList(100)

        int[] nextNSegmentIndex = [0, 1, 2, 3, 4, 5, 6]
        ArrayList<PersistValueMeta> returnPvmList = []

        def segmentBatch = new SegmentBatch(slot, snowFlake)
        def r = segmentBatch.splitAndTight(list, nextNSegmentIndex, returnPvmList)
        ArrayList<PersistValueMeta> somePvmList = returnPvmList[0..<10]

        and:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')

        def chunkDataFile = new File(Consts.slotDir, 'chunk-data-0')
        if (chunkDataFile.exists()) {
            chunkDataFile.delete()
        }

        def fdReadWriteForChunkSegments = new FdReadWrite('chunk_data_index_0', libC, chunkDataFile)
        fdReadWriteForChunkSegments.initByteBuffers(true)

        fdReadWriteForChunkSegments.writeSegment(segmentIndex, r[0].tightBytesWithLength, false)
        println 'write segment ' + segmentIndex

        def keyBucketsDataFile = new File(Consts.slotDir, 'key-bucket-split-0.dat')
        def fdReadWriteForKeyLoader = new FdReadWrite('key_loader_data', libC, keyBucketsDataFile)
        fdReadWriteForKeyLoader.initByteBuffers(false)
        // clear old data
        // segment index -> bucket index
        fdReadWriteForKeyLoader.writeSegment(bucketIndex, new byte[4096], false)

        and:
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, snowFlake)
        keyLoader.fdReadWriteArray = [fdReadWriteForKeyLoader]

        keyLoader.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber(slot, Consts.slotDir)
        keyLoader.metaKeyBucketSplitNumber.set(bucketIndex, (byte) 1)
        keyLoader.statKeyCountInBuckets = new StatKeyCountInBuckets(slot, keyLoader.bucketsPerSlot, Consts.slotDir)

        keyLoader.updatePvmListBatchAfterWriteSegments(walGroupIndex, somePvmList, false)
        println 'bucket ' + bucketIndex + ' key count: ' + keyLoader.getKeyCountInBucketIndex(bucketIndex)

        when:

        def wal = new Wal(slot, walGroupIndex, null, null, snowFlake)
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, wal)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L)

        var chunk = new Chunk(slot, snowFlake, Consts.slotDir, oneSlot, keyLoader, null)
        chunk.fdReadWriteArray = [fdReadWriteForChunkSegments]
        oneSlot.chunk = chunk

        var chunkMergeWorker = new ChunkMergeWorker(slot, oneSlot)

        ArrayList<Integer> needMergeSegmentIndexList = [segmentIndex]
        def job = new ChunkMergeJob(slot, needMergeSegmentIndexList, chunkMergeWorker, snowFlake)
        job.testTargetBucketIndex = bucketIndex
        job.mergeSegments(needMergeSegmentIndexList)

        then:
        job.validCvCountAfterRun == somePvmList.size()
        job.invalidCvCountAfterRun == list.size() - job.validCvCountAfterRun

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        keyLoader.metaKeyBucketSplitNumber.cleanUp()
        keyLoader.statKeyCountInBuckets.cleanUp()

        fdReadWriteForChunkSegments.cleanUp()
        fdReadWriteForKeyLoader.cleanUp()

        Consts.slotDir.delete()
    }
}
