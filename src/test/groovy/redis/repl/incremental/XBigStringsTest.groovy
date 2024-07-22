package redis.repl.incremental

import redis.CompressedValue
import redis.repl.BinlogContent
import spock.lang.Specification

import java.nio.ByteBuffer

class XBigStringsTest extends Specification {
    def 'test encode and decode'() {
        given:
        def uuid = 1L
        def key = 'test-big-string-key'
        def bytes = new byte[1024 * 200]

        def xBigStrings = new XBigStrings(uuid, key, bytes)

        expect:
        xBigStrings.type() == BinlogContent.Type.big_strings

        when:
        def encoded = xBigStrings.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        var xBigStrings2 = XBigStrings.decodeFrom(buffer)
        then:
        xBigStrings2.encodedLength() == encoded.length
        xBigStrings2.uuid == xBigStrings.uuid
        xBigStrings2.key == xBigStrings.key
        xBigStrings2.contentBytes == xBigStrings.contentBytes

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 8, (CompressedValue.KEY_MAX_LENGTH + 1).shortValue())
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 8, (short) 0)
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
