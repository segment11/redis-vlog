package redis.persist

import spock.lang.Specification

class StatKeyBucketLastUpdateCountTest extends Specification {
    private static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    def setup() {
        slotDir.mkdirs()
    }

    def "set and get"() {
        given:
        def one = new StatKeyBucketLastUpdateCount((byte) 0, 4096, slotDir)
        when:
        one.setKeyCountInBucketIndex(10, (short) 10, true)
        one.setKeyCountInBucketIndex(20, (short) 20, true)
        then:
        one.getKeyCountInBucketIndex(10) == 10
        one.getKeyCountInBucketIndex(20) == 20
        one.getKeyCount() == 30
        cleanup:
        one.clear()
        one.cleanUp()
    }
}
