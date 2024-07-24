package redis.type

import redis.BaseCommand
import spock.lang.Specification

import java.nio.ByteBuffer

class RedisHashKeysTest extends Specification {
    private static byte slot(String key) {
        BaseCommand.slot(key.bytes, 128).slot()
    }

    def 'key generate'() {
        expect:
        RedisHashKeys.keysKey('test') == 'h_k_{test}'
        RedisHashKeys.fieldKey('test', 'name') == 'h_f_{test}.name'

        slot('test') == slot('h_k_{test}')
        slot('test') == slot('h_f_{test}.name')
    }

    def 'set'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')

        then:
        rhk.contains('field1')
        rhk.size() == 1

        when:
        rhk.add('field2')

        then:
        rhk.contains('field2')
        rhk.size() == 2

        when:
        rhk.remove('field1')

        then:
        !rhk.contains('field1')
        rhk.size() == 1

        rhk.set == new HashSet(['field2'])
    }

    def 'encode'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')

        def encoded = rhk.encode()
        def rhk2 = RedisHashKeys.decode(encoded)

        then:
        rhk2.contains('field1')
        rhk2.contains('field2')
        rhk2.size() == 2
        RedisHashKeys.setSize(encoded) == 2
    }

    def 'decode crc32 not match'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')

        def encoded = rhk.encode()
        encoded[3] = 0

        boolean exception = false
        try {
            def rhk2 = RedisHashKeys.decode(encoded)
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception
    }

    def 'encode size 0'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        def encoded = rhk.encode()
        def rhk2 = RedisHashKeys.decode(encoded, false)

        then:
        rhk2.size() == 0

        when:
        rhk.add('field1')
        def encoded2 = rhk.encode()
        def rhk3 = RedisHashKeys.decode(encoded2, false)

        then:
        rhk3.size() == 1
    }

    def 'decode illegal length'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')

        def encoded = rhk.encode()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.putShort(6, (short) 0)

        boolean exception = false
        try {
            def rhk2 = RedisHashKeys.decode(encoded, false)
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception
    }
}
