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

        def memberA = rz.get('a')
        memberA.score(1)

        then:
        !rz.isEmpty()
        rz.size() == 3
        rz.getMemberMap().size() == 3
        rz.contains('a')
        memberA.initRank == 0
        !rz.remove('d')

        !rz.add(0.9, 'a', false, false)
        rz.add(1.8, 'b', true, true)

        rz.between(1, true, 2, true).collect { it.member() }.join(',') == 'a,b'
        rz.betweenByMember('a', true, 'b', true).collect { it.value.score() }.join(',') == '1.0,1.8'
        rz.between(0.9, true, 1.8, true).collect { it.member() }.join(',') == 'a,b'

        when:
        def isDeletedA = rz.remove('a')

        then:
        isDeletedA
        rz.size() == 2
        !rz.contains('a')

        when:
        rz.add(4, 'd')
        rz.add(5, 'e')
        rz.print()
        println rz.toString()

        then:
        rz.size() == 4
        rz.getSet().size() == 4
        rz.contains('d')
        rz.contains('e')

        rz.pollFirst().member() == 'b'
        rz.pollLast().member() == 'e'

        rz.pollLast().member() == 'd'
        rz.pollLast().member() == 'c'

        rz.pollFirst() == null
        rz.pollLast() == null

        when:
        rz.clear()

        then:
        rz.size() == 0
    }

    def 'encode'() {
        given:
        var rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')

        def encoded = rz.encode()
        def rz2 = RedisZSet.decode(encoded, false)
        def rz3 = RedisZSet.decode(encoded)

        then:
        rz2.size() == 3
        rz3.size() == 3
        RedisZSet.zsetSize(encoded) == 3
        rz2.contains('a')
        rz2.contains('b')
        rz2.contains('c')

        rz2.get('a').score() == 1
        rz2.get('b').score() == 2
        rz2.get('c').score() == 3
    }

    def 'decode crc32 not match'() {
        given:
        var rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')

        def encoded = rz.encode()
        encoded[2] = Byte.MAX_VALUE

        boolean exception = false
        try {
            def rz2 = RedisZSet.decode(encoded, true)
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception
    }

    def 'max score member'() {
        given:
        var rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(2, 'zz')
        rz.add(3, 'c')
        rz.add(3, 'zz')

        then:
        rz.pollLast().member() == 'zz'
    }

    def 'encode size 0'() {
        given:
        def rz = new RedisZSet()

        when:
        def encoded = rz.encode()
        def rz2 = RedisZSet.decode(encoded, false)

        then:
        rz2.size() == 0
    }
}
