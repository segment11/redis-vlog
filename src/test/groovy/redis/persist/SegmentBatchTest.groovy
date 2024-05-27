package redis.persist

import redis.SnowFlake
import redis.metric.ViewSupport
import spock.lang.Specification

class SegmentBatchTest extends Specification {
    def "split"() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch((byte) 0, (byte) 0, (byte) 0, snowFlake)

        var list = Mock.prepareShortValueList(800)

        int[] nextNSegmentIndex = [0, 1, 2, 3, 4, 5]
        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch.splitAndTight(list, nextNSegmentIndex, returnPvmList)
        for (one in r) {
            println one
        }

        println ViewSupport.format()

        then:
        returnPvmList.size() == list.size()
    }
}
