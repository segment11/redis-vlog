package redis.persist

import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentIndexTest extends Specification {
    def "set and get"() {
        given:
        def one = new MetaChunkSegmentIndex((byte) 0, slotDir)
        when:
        one.set(10)
        then:
        one.get() == 10

        when:
        one.set(20)
        then:
        one.get() == 20

        when:
        one.clear()
        then:
        one.get() == 0

        cleanup:
        one.cleanUp()
    }
}
