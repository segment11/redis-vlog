package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class ChunkMergeJobTest extends Specification {
    def 'merge segments'() {
        given:

        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch((byte) 0, snowFlake)

        var list = Mock.prepareValueList(100)

        int[] nextNSegmentIndex = [0, 1, 2, 3, 4, 5, 6]
        ArrayList<PersistValueMeta> returnPvmList = []

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

        fdReadWriteForChunkSegments.writeSegment(0, r[0].tightBytesWithLength, false)
        println 'write segment 0'

        def keyBucketsDataFile = new File(Consts.slotDir, 'key-bucket-split-0.dat')
        def fdReadWriteForKeyLoader = new FdReadWrite('key_loader_data', libC, keyBucketsDataFile)
        fdReadWriteForKeyLoader.initByteBuffers(false)
        // clear old data
        // segment index -> bucket index
        fdReadWriteForKeyLoader.writeSegment(0, new byte[4096], false)

        and:

        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def keyLoader = new KeyLoader((byte) 0, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, snowFlake)
        keyLoader.fdReadWriteArray = [fdReadWriteForKeyLoader]

        keyLoader.metaKeyBucketSplitNumber = new MetaKeyBucketSplitNumber((byte) 0, Consts.slotDir)
        keyLoader.metaKeyBucketSplitNumber.set(0, (byte) 1)
        keyLoader.statKeyCountInBuckets = new StatKeyCountInBuckets((byte) 0, keyLoader.bucketsPerSlot, Consts.slotDir)

        keyLoader.updatePvmListBatchAfterWriteSegments(0, somePvmList, false)
        println 'bucket 0 key count: ' + keyLoader.getKeyCountInBucketIndex(0)

        when:

        def wal = new Wal((byte) 0, 0, null, null, snowFlake)
        def oneSlot = new OneSlot((byte) 0, Consts.slotDir, keyLoader, wal)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L)

        var chunk = new Chunk((byte) 0, snowFlake, Consts.slotDir, oneSlot, keyLoader, null)
        chunk.fdReadWriteArray = [fdReadWriteForChunkSegments]
        oneSlot.chunk = chunk

        var chunkMergeWorker = new ChunkMergeWorker((byte) 0, oneSlot)

        ArrayList<Integer> needMergeSegmentIndexList = [0]
        def job = new ChunkMergeJob((byte) 0, needMergeSegmentIndexList, chunkMergeWorker, keyLoader.snowFlake)
        job.testTargetBucketIndex = 0
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
