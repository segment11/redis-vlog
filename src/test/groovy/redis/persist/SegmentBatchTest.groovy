package redis.persist

import redis.KeyHash
import redis.SnowFlake
import spock.lang.Specification

class SegmentBatchTest extends Specification {
    def "split"() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch((byte) 0, (byte) 0, (byte) 0, snowFlake)

        ArrayList<Wal.V> list = []
        int number = 200
        number.times {
            // like redis-benchmark key generator
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes
            def putValueBytes = ("value" + it).bytes

            def keyHash = KeyHash.hash(keyBytes)

            list << new Wal.V((byte) 0, 0L, 0, keyHash, 0,
                    key, putValueBytes, putValueBytes.length)
        }

        int[] nextNSegmentIndex = [0, 1, 2]
        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch.splitAndTight(list, nextNSegmentIndex, returnPvmList)
        for (one in r) {
            println one
        }

        then:
        returnPvmList.size() == list.size()
    }
}
