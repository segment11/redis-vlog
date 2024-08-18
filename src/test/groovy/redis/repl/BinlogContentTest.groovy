package redis.repl

import redis.CompressedValue
import redis.Dict
import redis.KeyHash
import redis.persist.Mock
import redis.repl.incremental.XBigStrings
import redis.repl.incremental.XDict
import redis.repl.incremental.XFlush
import redis.repl.incremental.XWalV
import spock.lang.Specification

import java.nio.ByteBuffer

class BinlogContentTest extends Specification {
    def 'test type'() {
        given:
        def one = BinlogContent.Type.fromCode(BinlogContent.Type.wal.code())

        expect:
        one == BinlogContent.Type.wal

        when:
        boolean exception = false
        try {
            BinlogContent.Type.fromCode((byte) 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def v = Mock.prepareValueList(1)[0]
        def xWalV = new XWalV(v, true, 0)
        def encoded = xWalV.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        def xWalV11 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XWalV
        then:
        xWalV11.v.encode() == v.encode()

        when:
        def uuid = 1L
        def key = 'test-big-string-key'
        def cv = new CompressedValue()
        cv.keyHash = KeyHash.hash(key.bytes)
        def cvEncoded = cv.encode()

        def xBigStrings = new XBigStrings(uuid, key, cvEncoded)
        encoded = xBigStrings.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xBigStrings2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XBigStrings
        then:
        xBigStrings2.encodedLength() == encoded.length

        when:
        def keyPrefix = 'key:'
        def dictBytes = new byte[300]
        def dict = new Dict(dictBytes)
        def xDict = new XDict(keyPrefix, dict)
        encoded = xDict.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xDict2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XDict
        then:
        xDict2.encodedLength() == encoded.length

        when:
        def xFlush = new XFlush()
        encoded = xFlush.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xFlush2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XFlush
        then:
        xFlush2.encodedLength() == encoded.length
    }
}
