package redis.type

import spock.lang.Specification

class RedisHHTest extends Specification {
    def 'hash'() {
        given:
        def rh = new RedisHH()

        when:
        rh.put('name', 'zhangsan'.bytes)
        rh.put('age', '20'.bytes)

        then:
        rh.get('name') == 'zhangsan'.bytes
        rh.get('age') == '20'.bytes
        rh.size() == 2

        when:
        rh.putAll(['name2': 'lisi'.bytes, 'age2': '30'.bytes])

        then:
        rh.get('name2') == 'lisi'.bytes
        rh.get('age2') == '30'.bytes
        rh.size() == 4
    }

    def 'encode'() {
        given:
        def rh = new RedisHH()

        when:
        rh.put('name', 'zhangsan'.bytes)
        rh.put('age', '20'.bytes)

        def encoded = rh.encode()
        def rh2 = RedisHH.decode(encoded)

        then:
        rh2.get('name') == 'zhangsan'.bytes
        rh2.get('age') == '20'.bytes
        rh2.size() == 2
    }
}
