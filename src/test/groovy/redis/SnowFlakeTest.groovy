package redis

import spock.lang.Specification

class SnowFlakeTest extends Specification {
    def 'NextId'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        List<Long> idSet = []
        def times = 1 << (10 + 10)

        when:
        def beginT = System.nanoTime()
        for (int i = 0; i < times; i++) {
            def id = snowFlake.nextId()
            if (i == 0 || i == times - 1) {
                idSet << id
            }
        }
        def endT = System.nanoTime()
        println 'Times: ' + times + ', Cost time: ' + (endT - beginT) / 1000 + 'us'

        then:
        idSet.size() == 2
        idSet.last() > idSet.first()
    }
}
