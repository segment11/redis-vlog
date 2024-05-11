package redis.persist

import spock.lang.Specification

class PersistValueMetaTest extends Specification {
    def "encode"() {
        given:
        def one = new PersistValueMeta()
        one.workerId = (byte) 0
        one.slot = (byte) 0
        one.batchIndex = (byte) 0
        one.subBlockIndex = (byte) 0
        one.length = 100
        one.segmentIndex = 10
        one.segmentOffset = 10

        when:
        def encoded = one.encode()

        then:
        PersistValueMeta.isPvm(encoded)
        PersistValueMeta.decode(encoded).toString() == one.toString()
    }
}
