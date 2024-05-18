package redis.persist

import spock.lang.Specification

class MetaKeyBucketSplitNumberTest extends Specification {
    private static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    def setup() {
        slotDir.mkdirs()
    }

    def "set and get"() {
        given:
        def one = new MetaKeyBucketSplitNumber((byte) 0, 4096, slotDir)
        when:
        one.set(10, (byte) 3)
        one.set(20, (byte) 9)
        one.set(30, (byte) 27)
        then:
        one.get((byte) 10) == 3
        one.get((byte) 20) == 9
        one.get((byte) 30) == 27
        cleanup:
        one.clear()
        one.cleanUp()
    }
}