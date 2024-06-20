package redis

import spock.lang.Specification

import java.nio.ByteBuffer

class DictTest extends Specification {
    def 'test all'() {
        given:
        def dict = new Dict()
        println dict
        println dict.hashCode()

        dict.seq = Dict.SELF_ZSTD_DICT_SEQ
        dict.dictBytes = new byte[10]

        def dict2 = new Dict()
        dict2.seq = 0

        expect:
        Dict.SELF_ZSTD_DICT.seq == Dict.SELF_ZSTD_DICT_SEQ
        dict.equals(dict)
        dict == Dict.SELF_ZSTD_DICT
        !dict.equals(null)
        dict != new String('xxx')
        dict != dict2
    }

    def 'test decode'() {
        given:
        def dict = new Dict(new byte[10])
        def keyPrefix = 'test'

        def encoded = dict.encode(keyPrefix)

        def dictWithKeyPrefix = Dict.decode(new DataInputStream(new ByteArrayInputStream(encoded)))
        println dictWithKeyPrefix.toString()
        def dict2 = dictWithKeyPrefix.dict

        expect:
        dict.encodeLength(keyPrefix) == encoded.length
        dictWithKeyPrefix.keyPrefix == keyPrefix
        dict == dict2
        dict2.dictBytes == dict.dictBytes

        when:
        def dict3 = Dict.decode(new DataInputStream(new ByteArrayInputStream(new byte[3])))

        then:
        dict3 == null

        when:
        def dict4 = Dict.decode(new DataInputStream(new ByteArrayInputStream(new byte[4])))

        then:
        dict4 == null

        when:
        byte[] testEncodedBytes = new byte[encoded.length]
        def buffer = ByteBuffer.wrap(testEncodedBytes)
        buffer.putInt(0, encoded.length - 4)
        buffer.putShort(4 + 4 + 8, (short) (CompressedValue.KEY_MAX_LENGTH + 1))

        boolean exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception

        when:
        buffer.putShort(4 + 4 + 8, (short) 0)

        exception = false

        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception

        when:
        buffer.putInt(0, encoded.length - 4 - 1)
        buffer.putShort(4 + 4 + 8, (short) keyPrefix.length())

        exception = false

        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception

        when:
        exception = false

        try {
            def dict5 = new Dict(new byte[Short.MAX_VALUE + 1])
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception
    }
}
