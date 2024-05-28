package redis.persist

import spock.lang.Specification
import static Consts.*

class StatKeyBucketLastUpdateCountTest extends Specification {
    def "set and get"() {
        given:
        def one = new StatKeyBucketLastUpdateCount((byte) 0, 4096, slotDir)
        when:
        one.setKeyCountInBucketIndex(10, (short) 10)
        one.setKeyCountInBucketIndex(20, (short) 20)
        then:
        one.getKeyCountInBucketIndex(10) == 10
        one.getKeyCountInBucketIndex(20) == 20
        one.getKeyCount() == 30
        cleanup:
        one.clear()
        one.cleanUp()
    }
}
