package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.CompressedValue
import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class ChunkMergeWorkerTest extends Specification {
    def 'test persist merged cv list'() {
        given:
        final byte slot = 0
        final int bucketIndex = 0
        int segmentIndex = 0
        final int walGroupIndex = 0
        def snowFlake = new SnowFlake(1, 1)

        and:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')

        and:
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, snowFlake)
        keyLoader.initFds(libC)

        and:

        def wal = new Wal(slot, walGroupIndex, null, null, snowFlake)
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, wal)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, walGroupIndex)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(segmentIndex + 1, Chunk.SEGMENT_FLAG_REUSE_AND_PERSISTED, 1L, walGroupIndex)

        var chunk = new Chunk(slot, Consts.slotDir, oneSlot, snowFlake, keyLoader, null)
        oneSlot.chunk = chunk
        chunk.initSegmentIndexWhenFirstStart(segmentIndex)
        chunk.initFds(libC)

        when:
        var chunkMergeWorker = new ChunkMergeWorker(slot, oneSlot)
        chunkMergeWorker.MERGED_CV_SIZE_THRESHOLD = 1000
        def isPersisted = chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk()

        then:
        !isPersisted

        when:
        var bucketIndexTarget = bucketIndex
        var bucketIndexTarget2 = bucketIndex + ConfForSlot.global.confWal.oneChargeBucketNumber

        ConfForSlot.global.confBucket.bucketsPerSlot = 4096
        def keyHashByBucketIndex = Mock.prepareKeyHashIndexByKeyBucketList(3_000_000, ConfForSlot.global.confBucket.bucketsPerSlot)
        def cvList1 = keyHashByBucketIndex[bucketIndexTarget][0..<500].collect {
            def cv = new CompressedValue()
            cv.seq = it.v2
            cv.keyHash = it.v2
            cv.compressedData = new byte[10]
            cv.compressedLength = 10
            cv.uncompressedLength = 10

            new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, it.v1, bucketIndexTarget, segmentIndex)
        }
        def cvList2 = keyHashByBucketIndex[bucketIndexTarget2][0..<500].collect {
            def cv = new CompressedValue()
            cv.seq = it.v2
            cv.keyHash = it.v2
            cv.compressedData = new byte[10]
            cv.compressedLength = 10
            cv.uncompressedLength = 10

            new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, it.v1, bucketIndexTarget2, segmentIndex)
        }

        for (one in cvList1) {
            chunkMergeWorker.addMergedCv(one)
        }
        for (one in cvList2) {
            chunkMergeWorker.addMergedCv(one)
        }

        // key loader need write these keys first, because after chunk persist, key loader will update only the keys in the bucket
        List<PersistValueMeta> pvmList1 = []
        List<PersistValueMeta> pvmList2 = []
        cvList1[0..<KeyBucket.INIT_CAPACITY].each {
            def cv = it.cv

            def pvm = new PersistValueMeta()
            pvm.expireAt = cv.expireAt
            pvm.seq = cv.seq
            pvm.keyBytes = it.key.bytes
            pvm.keyHash = cv.keyHash
            pvm.bucketIndex = it.bucketIndex
            pvm.extendBytes = cv.encode()

            pvmList1 << pvm
        }
        cvList2[0..<KeyBucket.INIT_CAPACITY].each {
            def cv = it.cv

            def pvm = new PersistValueMeta()
            pvm.expireAt = cv.expireAt
            pvm.seq = cv.seq
            pvm.keyBytes = it.key.bytes
            pvm.keyHash = cv.keyHash
            pvm.bucketIndex = it.bucketIndex
            pvm.extendBytes = cv.encode()

            pvmList2 << pvm
        }
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList1)
        keyLoader.updatePvmListBatchAfterWriteSegments(1, pvmList2)
        println 'prepare key size: ' + (pvmList1.size() + pvmList2.size())

        def keyCount1 = keyLoader.getKeyCountInBucketIndex(bucketIndexTarget)
        def keyCount2 = keyLoader.getKeyCountInBucketIndex(bucketIndexTarget2)
        println 'key count: ' + keyCount1 + ', ' + keyCount2

        isPersisted = chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk()
        println 'chunk persist fd length: ' + (chunk.fdLengths[0] / 1024) + ' KB'

        keyCount1 = keyLoader.getKeyCountInBucketIndex(bucketIndexTarget)
        keyCount2 = keyLoader.getKeyCountInBucketIndex(bucketIndexTarget2)
        println 'after merge key count: ' + keyCount1 + ', ' + keyCount2

        def firstCv = cvList1[0]
        println 'first cv key: ' + firstCv.key
        def firstValueBytes = keyLoader.getValueByKey(bucketIndexTarget, firstCv.key.bytes, firstCv.cv.keyHash)
        def pvmAfterMerge = PersistValueMeta.decode(firstValueBytes.valueBytes)
        println 'pvm after merge: ' + pvmAfterMerge

        def lastCv = cvList2[KeyBucket.INIT_CAPACITY - 1]
        println 'last cv key: ' + lastCv.key
        def lastValueBytes = keyLoader.getValueByKey(bucketIndexTarget2, lastCv.key.bytes, lastCv.cv.keyHash)
        def pvmAfterMerge2 = PersistValueMeta.decode(lastValueBytes.valueBytes)
        println 'pvm after merge: ' + pvmAfterMerge2

        then:
        isPersisted
        keyCount1 == KeyBucket.INIT_CAPACITY
        keyCount2 == KeyBucket.INIT_CAPACITY

        cleanup:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        keyLoader.cleanUp()

        Consts.slotDir.deleteDir()
    }

    def 'before persist wal'() {
        given:
        final byte slot = 0

        var chunkMergeWorker = new ChunkMergeWorker(slot, null)

        def cvList = Mock.prepareCompressedValueList(10)
        for (cv in cvList) {
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, 'key' + cv.seq, 0, 0))
        }
        chunkMergeWorker.addMergedSegment(0, 10)

        when:
        def ext2 = chunkMergeWorker.getMergedButNotPersistedBeforePersistWal(0)

        then:
        ext2.segmentIndexList().size() == 1
        ext2.vList().size() == 10

        when:
        chunkMergeWorker.removeMergedButNotPersistedAfterPersistWal([0], 0)

        then:
        chunkMergeWorker.mergedCvList.size() == 0
        chunkMergeWorker.mergedSegmentSet.size() == 0
    }

}
