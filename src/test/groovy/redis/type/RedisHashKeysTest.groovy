package redis.type

import redis.BaseCommand
import spock.lang.Specification

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
    }
}
