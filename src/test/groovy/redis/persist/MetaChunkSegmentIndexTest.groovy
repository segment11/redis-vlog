package redis.persist

import spock.lang.Specification

class MetaChunkSegmentIndexTest extends Specification {
    private static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    def setup() {
        slotDir.mkdirs()
    }

    def "set and get"() {
        given:
        def one = new MetaChunkSegmentIndex((byte) 0, (byte) 2, slotDir)
        when:
        one.put((byte) 0, (byte) 0, 10)
        one.put((byte) 0, (byte) 1, 20)
        one.put((byte) 1, (byte) 0, 100)
        one.put((byte) 1, (byte) 1, 200)
        then:
        one.get((byte) 0, (byte) 0) == 10
        one.get((byte) 0, (byte) 1) == 20
        one.get((byte) 1, (byte) 0) == 100
        one.get((byte) 1, (byte) 1) == 200
        cleanup:
        one.clear()
        one.cleanUp()
    }
}
