package redis.type

import spock.lang.Specification

class RedisZSetTest extends Specification {
    def 'zset'() {
        given:
        def rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')

        then:
        !rz.isEmpty()
        rz.size() == 3
        rz.contains('a')

        !rz.add(0.9, 'a', false, false)
        rz.add(1.8, 'b', true, true)

        rz.between(1, true, 2, true).collect { it.member() }.join(',') == 'a,b'
        rz.betweenByMember('a', true, 'b', true).collect { it.value.score() }.join(',') == '1.0,1.8'

        when:
        rz.remove('a')

        then:
        rz.size() == 2
        !rz.contains('a')

        when:
        rz.add(4, 'd')
        rz.add(5, 'e')

        then:
        rz.size() == 4
        rz.contains('d')
        rz.contains('e')

        rz.pollFirst().member() == 'b'
        rz.pollLast().member() == 'e'
    }

    def 'encode'() {
        given:
        var rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')

        def encoded = rz.encode()
        def rz2 = RedisZSet.decode(encoded)

        then:
        rz2.size() == 3
        rz2.contains('a')
        rz2.contains('b')
        rz2.contains('c')

        rz2.get('a').score() == 1
        rz2.get('b').score() == 2
        rz2.get('c').score() == 3
    }
}
