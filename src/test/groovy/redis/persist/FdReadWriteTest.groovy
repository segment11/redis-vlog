package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import spock.lang.Specification

import java.nio.ByteBuffer

class FdReadWriteTest extends Specification {
    def "test multi read write"() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        def oneFile = new File('/tmp/test-fd-read-write-multi')
        if (oneFile.exists()) {
            oneFile.delete()
        }
        def oneFile2 = new File('/tmp/test-fd-read-write2')
        if (oneFile2.exists()) {
            oneFile2.delete()
        }

        and:
        def fdReadWrite = new FdReadWrite('test', libC, oneFile)
        fdReadWrite.initByteBuffers(true)

        def fdReadWrite2 = new FdReadWrite('test2', libC, oneFile2)
        fdReadWrite2.initByteBuffers(true)

        def segmentLength = ConfForSlot.global.confChunk.segmentLength

        int loop = 10

        when:
        int[] array = new int[loop * 2]
        loop.times { i ->
            var bytes = new byte[segmentLength]
            Arrays.fill(bytes, (byte) i)
            var f = fdReadWrite.writeSegment(i, bytes, false)
            var f2 = fdReadWrite2.writeSegment(i, bytes, false)
            array[i] = f
            array[i + loop] = f2
        }

        then:
        array.every { it == segmentLength }

        cleanup:
        fdReadWrite.cleanUp()
        fdReadWrite2.cleanUp()
    }

    def "test read write batch"() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        def oneFile = new File('/tmp/test-fd-read-write-batch')
        if (oneFile.exists()) {
            oneFile.delete()
        }

        and:
        def fdReadWrite = new FdReadWrite('test', libC, oneFile)
        fdReadWrite.initByteBuffers(true)

        def segmentLength = ConfForSlot.global.confChunk.segmentLength

        when:
        var bytesBatch = new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE]
        ByteBuffer.wrap(bytesBatch).putInt(0, 100)

        println 'write bytes n: ' + fdReadWrite.writeSegmentBatch(0, bytesBatch, false)
        println 'write bytes n: ' + fdReadWrite.writeSegmentBatch(100, bytesBatch, false)
        println 'write bytes n: ' + fdReadWrite.writeSegmentBatch(104, bytesBatch, false)
        println 'write bytes n: ' + fdReadWrite.writeSegmentBatch(108, bytesBatch, false)

        def firstSegmentBytes = fdReadWrite.readSegment(0, false)
        def index100SegmentBytesForMerge = fdReadWrite.readSegmentForMerge(100, FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT)
        def index100SegmentBytesForMergeJust2Segment = fdReadWrite.readSegmentForMerge(100, 2)
        def index100SegmentBytes = fdReadWrite.readSegment(100, false)

        then:
        firstSegmentBytes.length == segmentLength
        index100SegmentBytesForMerge.length == segmentLength * FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT
        index100SegmentBytesForMergeJust2Segment.length == segmentLength * 2
        ByteBuffer.wrap(firstSegmentBytes).getInt() == 100
        ByteBuffer.wrap(index100SegmentBytes).getInt() == 100

        cleanup:
        fdReadWrite.cleanUp()
    }

    def 'test skip segment write'() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        def oneFile = new File('/tmp/test-fd-read-write-skip-segment-write')
        if (oneFile.exists()) {
            oneFile.delete()
        }

        and:
        def fdReadWrite = new FdReadWrite('test', libC, oneFile)
        fdReadWrite.initByteBuffers(true)

        def segmentLength = ConfForSlot.global.confChunk.segmentLength

        and:
        def bytes0 = new byte[segmentLength]

        def segmentIndex20Bytes = new byte[segmentLength]
        Arrays.fill(segmentIndex20Bytes, (byte) 20)
        def segmentIndex30Bytes = new byte[segmentLength]
        Arrays.fill(segmentIndex30Bytes, (byte) 30)
        def segmentIndex100Bytes = new byte[segmentLength]
        Arrays.fill(segmentIndex100Bytes, (byte) 100)

        when:
        fdReadWrite.writeSegment(20, segmentIndex20Bytes, false)
        fdReadWrite.writeSegment(30, segmentIndex30Bytes, false)
        fdReadWrite.writeSegment(100, segmentIndex100Bytes, false)

        def writeIndex = fdReadWrite.writeIndex

        def readSegment20Bytes = fdReadWrite.readSegment(20, false)
        def readSegment30Bytes = fdReadWrite.readSegment(30, false)
        def readSegment100Bytes = fdReadWrite.readSegment(100, false)

        List<byte[]> readFirst10SegmentBytesList = []
        (0..<10).each {
            readFirst10SegmentBytesList << fdReadWrite.readSegment(it, false)
        }
        [11, 31, 99].each {
            readFirst10SegmentBytesList << fdReadWrite.readSegment(it, false)
        }
        def readSegment101Bytes = fdReadWrite.readSegment(101, false)

        then:
        writeIndex == (100 + 1) * segmentLength

        readSegment20Bytes == segmentIndex20Bytes
        readSegment30Bytes == segmentIndex30Bytes
        readSegment100Bytes == segmentIndex100Bytes
        readFirst10SegmentBytesList.every {
            it == bytes0
        }
        readSegment101Bytes == null

        cleanup:
        fdReadWrite.cleanUp()
    }

    def 'test segment index overflow'() {
        given:
        def fdReadWrite = new FdReadWrite('test')

        when:
        def isOverflow = false
        fdReadWrite.isChunkFd = true
        try {
            fdReadWrite.writeSegment(ConfForSlot.global.confChunk.segmentNumberPerFd, new byte[0], false)
        } catch (AssertionError e) {
            isOverflow = true
        }

        then:
        isOverflow

        when:
        isOverflow = false
        fdReadWrite.isChunkFd = false
        try {
            fdReadWrite.writeSegment(ConfForSlot.global.confBucket.bucketsPerSlot, new byte[0], false)
        } catch (AssertionError e) {
            isOverflow = true
        }

        then:
        isOverflow
    }
}
