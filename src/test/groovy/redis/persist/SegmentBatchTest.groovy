package redis.persist

import com.github.luben.zstd.Zstd
import redis.CompressedValue
import redis.SnowFlake
import redis.metric.ViewSupport
import spock.lang.Specification

import java.nio.ByteBuffer

class SegmentBatchTest extends Specification {
    def "tight segments write and read"() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch((byte) 0, snowFlake)

        var list = Mock.prepareValueList(400)

        int[] nextNSegmentIndex = [0, 1, 2, 3, 4, 5, 6]
        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch.splitAndTight(list, nextNSegmentIndex, returnPvmList)
        for (one in r) {
            println one
        }

        def first = r[0]
        def buffer = ByteBuffer.wrap(first.tightBytesWithLength())
        def seq = buffer.getLong()
        def totalBytesN = buffer.getInt()

        println "seq: $seq, total bytes: $totalBytesN"

        List<CompressedValue> loaded = []

        for (i in 0..<SegmentBatch.MAX_BLOCK_NUMBER) {
            buffer.position(8 + 4 + i * (2 + 2))
            def offset = buffer.getShort()
            def length = buffer.getShort()

            if (offset == 0) {
                assert length == 0
                break
            }

            def compressedBytes = new byte[length]
            buffer.position(offset).get(compressedBytes)

            println "offset: $offset, compressed bytes length: ${compressedBytes.length}"

            def decompressedBytes = Zstd.decompress(compressedBytes, 4096)
            println "decompressed bytes length: ${decompressedBytes.length}"

            SegmentBatch.iterateFromSegmentBytes(decompressedBytes, { key, cv, offsetInThisSegment ->
                if (cv.seq % 10 == 0) {
                    println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
                }
                loaded << cv
            })
        }

        println ViewSupport.format()

        then:
        returnPvmList.size() == list.size()

        loaded.every { one ->
            one.compressedLength() == 10 &&
                    list.find { it.seq == one.seq }.keyHash == one.keyHash
        }
    }
}
