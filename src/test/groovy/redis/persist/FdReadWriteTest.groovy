package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import org.apache.commons.io.FileUtils
import redis.ConfForSlot
import redis.repl.content.ToMasterExistsChunkSegments
import spock.lang.Specification

import java.nio.ByteBuffer

class FdReadWriteTest extends Specification {
    def 'test write and read'() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        def oneFile1 = new File('/tmp/test-fd-read-write-chunk')
        if (oneFile1.exists()) {
            oneFile1.delete()
        }
        def oneFile2 = new File('/tmp/test-fd-read-write-key-bucket')
        if (!oneFile2.exists()) {
            FileUtils.touch(oneFile2)
        }
        def oneFile11 = new File('/tmp/test-fd-read-write-chunk2')
        if (oneFile11.exists()) {
            oneFile11.delete()
        }
        def oneFile22 = new File('/tmp/test-fd-read-write-key-bucket2')
        if (!oneFile22.exists()) {
            FileUtils.touch(oneFile22)
        }

        // chunk segment length same with one key bucket cost length
        ConfForSlot.global.confChunk.segmentLength = KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        ConfForSlot.global.pureMemory = false
        ConfForSlot.global.confChunk.lruPerFd.maxSize = 10
        ConfForSlot.global.confBucket.lruPerFd.maxSize = 10

        and:
        def fdChunk = new FdReadWrite('test', libC, oneFile1)
        fdChunk.initByteBuffers(true)

        def fdKeyBucket = new FdReadWrite('test2', libC, oneFile2)
        fdKeyBucket.initByteBuffers(false)

        fdChunk.afterFdPreadCompressCountTotal = 1
        fdChunk.readCountTotal = 1
        fdChunk.writeCountTotal = 1
        fdChunk.lruHitCounter = 1
        fdChunk.lruMissCounter = 1
        FdReadWrite.fdReadWriteGauge.collect()

        def segmentLength = ConfForSlot.global.confChunk.segmentLength
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        when:
        // lru off
        ConfForSlot.global.confChunk.lruPerFd.maxSize = 0
        ConfForSlot.global.confBucket.lruPerFd.maxSize = 0
        def fdChunk11 = new FdReadWrite('test11', libC, oneFile11)
        def fdKeyBucket22 = new FdReadWrite('test22', libC, oneFile22)
        fdChunk11.initByteBuffers(true)
        fdKeyBucket22.initByteBuffers(false)
        then:
        fdChunk11 != null
        fdKeyBucket22 != null

        when:
        int loop = 10
        int[] array = new int[loop * 2]
        loop.times { i ->
            byte[] bytes
            if (i == 0) {
                bytes = new byte[segmentLength - 1]
            } else {
                bytes = new byte[segmentLength]
                Arrays.fill(bytes, (byte) i)
            }

            def f1 = fdChunk.writeOneInner(i, bytes, false)
            def f11 = fdChunk11.writeOneInner(i, bytes, false)
            def f2 = fdKeyBucket.writeOneInner(i, bytes, false)
            if (i == 9) {
                f1 = fdChunk.writeOneInner(i, bytes, true)
                f2 = fdKeyBucket.writeOneInner(i, bytes, true)
            }
            array[i] = f1
            array[i + loop] = f2
        }
        fdKeyBucket.writeOneInnerForKeyBucketsInOneWalGroup(1 * oneChargeBucketNumber, new byte[oneChargeBucketNumber * segmentLength])
        then:
        array.every { it == segmentLength }
        fdChunk.readOneInner(0, false).length == segmentLength
        fdChunk.readOneInner(0, true).length == segmentLength
        fdChunk.readOneInner(0, true).length == segmentLength
        fdChunk11.readOneInner(0, true).length == segmentLength
        fdChunk.readSegmentsForMerge(0, loop).length == segmentLength * loop
        fdChunk.readOneInnerForRepl(0).length == segmentLength * loop
        fdKeyBucket.readOneInner(0, false).length == segmentLength
        fdKeyBucket.readOneInner(0, true).length == segmentLength
        fdKeyBucket.readOneInner(0, true).length == segmentLength
        fdKeyBucket.readOneInnerForKeyBucketsInOneWalGroup(1 * oneChargeBucketNumber) != null

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE], false)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE - 1, false).length == segmentLength

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE], true)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE - 1, false).length == segmentLength

        when:
        boolean exception = false
        try {
            fdChunk.readOneInner(-1, false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.readOneInner(ConfForSlot.global.confChunk.segmentNumberPerFd, false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.readOneInner(-1, true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.readOneInner(ConfForSlot.global.confBucket.bucketsPerSlot, true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.readSegmentsForMerge(0, loop + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def oldWriteIndex = fdChunk.writeIndex
        fdChunk.writeIndex = 1
        def bytesForMerge = fdChunk.readSegmentsForMerge(10, loop)
        fdChunk.writeIndex = oldWriteIndex
        then:
        bytesForMerge == null

        when:
        oldWriteIndex = fdChunk.writeIndex
        fdChunk.writeIndex = 4096
        def bytesX = fdChunk.readSegmentsForMerge(0, 10)
        fdChunk.writeIndex = oldWriteIndex
        then:
        bytesX.length == 4096

        when:
        exception = false
        oldWriteIndex = fdChunk.writeIndex
        fdChunk.writeIndex = 10
        try {
            fdChunk.readSegmentsForMerge(0, 10)
        } catch (RuntimeException e) {
            println e.message
            exception = true
        } finally {
            fdChunk.writeIndex = oldWriteIndex
        }
        then:
        exception

        when:
        fdChunk.writeOneInnerForRepl(1024, new byte[segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT], 0)
        then:
        fdChunk.readOneInner(1024 + ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT - 1, false).length == segmentLength

        when:
        fdChunk.writeOneInnerForRepl(1024, new byte[segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT + 1], 1)
        then:
        fdChunk.readOneInner(1024 + ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT - 1, false).length == segmentLength

        when:
        fdChunk.truncate()
        fdChunk.cleanUp()
        fdKeyBucket.truncate()
        fdKeyBucket.cleanUp()

        ConfForSlot.global.pureMemory = true
        fdChunk = new FdReadWrite('test', libC, oneFile1)
        fdChunk.initByteBuffers(true)
        fdKeyBucket = new FdReadWrite('test2', libC, oneFile2)
        fdKeyBucket.initByteBuffers(false)
        then:
        fdChunk.isTargetSegmentIndexNullInMemory(0)

        when:
        loop.times { i ->
            def bytes = new byte[segmentLength]
            Arrays.fill(bytes, (byte) i)
            def f1 = fdChunk.writeOneInner(i, bytes, false)
            def f2 = fdKeyBucket.writeOneInner(i, bytes, false)
            array[i] = f1
            array[i + loop] = f2
        }
        fdKeyBucket.writeOneInnerForKeyBucketsInOneWalGroup(1 * oneChargeBucketNumber, new byte[oneChargeBucketNumber * segmentLength])
        then:
        !fdChunk.isTargetSegmentIndexNullInMemory(0)
        array.every { it == segmentLength }
        fdChunk.readOneInner(0, false).length == segmentLength
        fdChunk.readSegmentsForMerge(0, loop).length == segmentLength * loop
        fdChunk.readOneInnerForRepl(0).length == segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT
        fdKeyBucket.readOneInner(0, false).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readOneInnerForKeyBucketsInOneWalGroup(1 * oneChargeBucketNumber).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readOneInnerBatchFromMemory(1, 1).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readOneInnerBatchFromMemory(1, oneChargeBucketNumber).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readOneInnerBatchFromMemory(1, ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT).length ==
                segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT

        when:
        exception = false
        try {
            fdChunk.writeOneInner(0, new byte[segmentLength + 1], false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.writeSegmentsBatch(0, new byte[segmentLength], false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.writeOneInnerForKeyBucketsInOneWalGroup(0, new byte[10])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.writeOneInnerForRepl(0, new byte[10], 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE], false)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE - 1, false).length == segmentLength

        when:
        fdChunk.writeOneInnerForRepl(1024, new byte[segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT], 0)
        then:
        fdChunk.readOneInner(1024 + ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT - 1, false).length == segmentLength

        when:
        exception = false
        try {
            fdKeyBucket.readOneInnerBatchFromMemory(1, 2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.writeOneInnerBatchToMemory(0, new byte[10], 0)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def n = fdKeyBucket.writeOneInnerBatchToMemory(1024, new byte[segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT + 1], 1)
        then:
        n == segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT

        when:
        def replKeyBucketBytesOnceFromMaster = new byte[segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT]
        Arrays.fill(replKeyBucketBytesOnceFromMaster, (byte) 1)
        n = fdKeyBucket.writeOneInnerBatchToMemory(1024, replKeyBucketBytesOnceFromMaster, 0)
        then:
        n == segmentLength * ToMasterExistsChunkSegments.REPL_ONCE_CHUNK_SEGMENT_COUNT

        when:
        exception = false
        try {
            fdChunk.writeOneInnerBatchToMemory(1, new byte[4096], 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        fdChunk.writeOneInnerBatchToMemory(200, new byte[10], 0)
        then:
        fdChunk.readOneInner(200, false).length == 10

        when:
        fdKeyBucket.clearOneKeyBucketToMemory(oneChargeBucketNumber * 2)
        fdKeyBucket.clearOneKeyBucketToMemory(1)
        def keyBucket1BytesRead = new byte[segmentLength]
        ByteBuffer.wrap(fdKeyBucket.readOneInner(1, false)).get(segmentLength, keyBucket1BytesRead)
        then:
        keyBucket1BytesRead == new byte[segmentLength]

        when:
        fdKeyBucket.clearKeyBucketsInOneWalGroup(oneChargeBucketNumber)
        then:
        fdKeyBucket.readOneInnerForKeyBucketsInOneWalGroup(oneChargeBucketNumber) == null

        cleanup:
        fdChunk.truncate()
        fdChunk.cleanUp()
        fdKeyBucket.truncate()
        fdKeyBucket.cleanUp()
        fdChunk11.truncate()
        fdChunk11.cleanUp()
        fdKeyBucket22.truncate()
        fdKeyBucket22.cleanUp()
        oneFile1.delete()
        oneFile2.delete()
        oneFile11.delete()
        oneFile22.delete()
        ConfForSlot.global.pureMemory = false
    }
}
