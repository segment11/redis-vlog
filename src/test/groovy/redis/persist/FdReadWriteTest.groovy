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
        def oneFile = new File('/tmp/test-fd-read-write')
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
        def index100SegmentBytesForMerge = fdReadWrite.readSegmentForMerge(100)
        def index100SegmentBytes = fdReadWrite.readSegment(100, false)

        then:
        firstSegmentBytes.length == segmentLength
        index100SegmentBytesForMerge.length == segmentLength * FdReadWrite.MERGE_READ_ONCE_SEGMENT_COUNT
        ByteBuffer.wrap(firstSegmentBytes).getInt() == 100
        ByteBuffer.wrap(index100SegmentBytes).getInt() == 100

        cleanup:
        fdReadWrite.cleanUp()
    }
}
