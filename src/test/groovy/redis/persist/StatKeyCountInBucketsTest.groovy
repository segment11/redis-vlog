package redis.persist

import spock.lang.Specification
import static Consts.*

class StatKeyCountInBucketsTest extends Specification {
    def "set and get"() {
        given:
        def one = new StatKeyCountInBuckets((byte) 0, 4096, slotDir)
        when:
        one.setKeyCountForBucketIndex(10, (short) 10)
        one.setKeyCountForBucketIndex(20, (short) 20)
        then:
        one.getKeyCountForBucketIndex(10) == 10
        one.getKeyCountForBucketIndex(20) == 20
        one.getKeyCount() == 30
        cleanup:
        one.clear()
        one.cleanUp()
    }
}
