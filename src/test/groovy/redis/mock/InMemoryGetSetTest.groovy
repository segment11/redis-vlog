package redis.mock


import redis.persist.Mock
import spock.lang.Specification

class InMemoryGetSetTest extends Specification {
    def 'test set and get'() {
        given:
        final byte slot = 0
        def inMemoryGetSet = new InMemoryGetSet()

        when:
        def cvList = Mock.prepareCompressedValueList(10)
        for (cv in cvList) {
            inMemoryGetSet.put(slot, 'key' + cv.seq, 0, cv)
        }

        then:
        (0..<10).every {
            def bufOrCv = inMemoryGetSet.getBuf(slot, ('key' + it).bytes, 0, it)
            bufOrCv.cv == cvList[it]
        }

        inMemoryGetSet.getBuf(slot, 'key10'.bytes, 0, 10) == null
    }
}
