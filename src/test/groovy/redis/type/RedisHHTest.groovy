package redis.type

import redis.CompressedValue
import spock.lang.Specification

import java.nio.ByteBuffer

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
        rh.remove('name')
        !rh.remove('name')
        rh.size() == 1

        when:
        rh.putAll(['name2': 'lisi'.bytes, 'age2': '30'.bytes])
        then:
        rh.get('name2') == 'lisi'.bytes
        rh.get('age2') == '30'.bytes
        rh.size() == 3
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
        RedisHH.getSizeWithoutDecode(encoded) == 2
        rh2.map.containsKey('name')
        rh2.map.containsKey('age')

        when:
        int countOnlyFindName = 0
        RedisHH.iterate(encoded, true) { key, value ->
            countOnlyFindName++
            if (key == 'name') {
                return true
            } else {
                return false
            }
        }
        then:
        countOnlyFindName == 1
    }

    def 'decode crc32 not match'() {
        given:
        def rh = new RedisHH()

        when:
        rh.put('name', 'zhangsan'.bytes)
        rh.put('age', '20'.bytes)
        def encoded = rh.encode()
        encoded[2] = 0
        boolean exception = false
        try {
            RedisHH.decode(encoded)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'encode size 0'() {
        given:
        def rh = new RedisHH()

        when:
        def encoded = rh.encode()
        def rh2 = RedisHH.decode(encoded, false)
        then:
        rh2.size() == 0
    }

    def 'key or value length too long'() {
        given:
        def rh = new RedisHH()

        when:
        def key = 'a' * 1024
        def value = 'b'
        boolean exception = false
        try {
            rh.put(key, value.bytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        key = 'a'
        value = 'b' * 1024 * 1024
        boolean exception2 = false
        try {
            rh.put(key, value.bytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception2 = true
        }
        then:
        exception2
    }

    def 'decode key or value length error'() {
        given:
        def rh = new RedisHH()

        when:
        def key = 'a'
        def value = 'b'
        rh.put(key, value.bytes)
        def encoded = rh.encode()
        def buffer = ByteBuffer.wrap(encoded)
        // 6 -> header size short + crc32 int
        buffer.putShort(6, (short) (CompressedValue.KEY_MAX_LENGTH + 1))
        boolean exception = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putShort(6, (short) -1)
        exception = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putShort(6, (short) 1)
        buffer.putShort(6 + 2 + 1, (short) (CompressedValue.VALUE_MAX_LENGTH + 1))
        boolean exception2 = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception2 = true
        }
        then:
        exception2

        when:
        buffer.putShort(6 + 2 + 1, (short) -1)
        exception2 = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception2 = true
        }
        then:
        exception2
    }
}
