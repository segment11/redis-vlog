package redis.repl.incremental

import redis.CompressedValue
import redis.Dict
import redis.DictMap
import redis.persist.Consts
import redis.repl.BinlogContent
import spock.lang.Specification

import java.nio.ByteBuffer

class XDictTest extends Specification {
    def 'test encode and decode'() {
        given:
        def keyPrefix = 'key:'
        def dictBytes = new byte[300]
        var dict = new Dict(dictBytes)

        def xDict = new XDict(keyPrefix, dict)

        expect:
        xDict.type() == BinlogContent.Type.dict

        when:
        def encoded = xDict.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        var xDict2 = XDict.decodeFrom(buffer)
        then:
        xDict2.encodedLength() == encoded.length
        xDict2.keyPrefix == xDict.keyPrefix
        xDict2.dict.dictBytes == dict.dictBytes

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            XDict.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 4 + 8, (CompressedValue.KEY_MAX_LENGTH + 1).shortValue())
        buffer.position(1)
        try {
            XDict.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 4 + 8, (short) 0)
        buffer.position(1)
        try {
            XDict.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)
        xDict.apply((byte) 0)
        then:
        dictMap.getDict(keyPrefix).dictBytes == dict.dictBytes

        cleanup:
        dictMap.clearAll()
        dictMap.close()
    }
}
