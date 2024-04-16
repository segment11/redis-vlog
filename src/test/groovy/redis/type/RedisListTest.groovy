package redis.type

import spock.lang.Specification

class RedisListTest extends Specification {

    def 'list'() {
        given:
        def rl = new RedisList()

        when:
        rl.addFirst('a'.bytes)
        rl.addFirst('b'.bytes)
        rl.addFirst('c'.bytes)

        then:
        rl.size() == 3
        rl.get(0) == 'c'.bytes
        rl.get(1) == 'b'.bytes
        rl.get(2) == 'a'.bytes

        when:
        rl.addLast('d'.bytes)

        then:
        rl.size() == 4
        rl.get(3) == 'd'.bytes

        when:
        // c e b a d
        rl.addAt(1, 'e'.bytes)

        then:
        rl.size() == 5
        rl.get(1) == 'e'.bytes

        when:
        // c f b a d
        rl.setAt(1, 'f'.bytes)

        then:
        rl.size() == 5
        rl.get(1) == 'f'.bytes
        rl.indexOf('f'.bytes) == 1

        when:
        // f b a d
        rl.removeFirst()
        // f b a
        rl.removeLast()

        then:
        rl.size() == 3
        rl.get(0) == 'f'.bytes
        rl.get(2) == 'a'.bytes
    }

    def 'encode'() {
        given:
        def rl = new RedisList()

        when:
        rl.addFirst('a'.bytes)
        rl.addFirst('b'.bytes)
        rl.addFirst('c'.bytes)

        def encoded = rl.encode()
        def rl2 = RedisList.decode(encoded)

        then:
        rl2.size() == 3
        rl2.get(0) == 'c'.bytes
        rl2.get(1) == 'b'.bytes
        rl2.get(2) == 'a'.bytes
    }
}
