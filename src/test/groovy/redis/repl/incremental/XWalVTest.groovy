package redis.repl.incremental

import redis.CompressedValue
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.persist.Mock
import redis.repl.BinlogContent
import redis.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XWalVTest extends Specification {
    def 'test encode and decode'() {
        given:
        def v = Mock.prepareValueList(1)[0]
        def xWalV = new XWalV(v, true, 0)
        def xWalV2 = new XWalV(v, false, 0)
        def xWalV3 = new XWalV(v)
        println xWalV3.v

        expect:
        xWalV.type() == BinlogContent.Type.wal

        when:
        def encoded = xWalV.encodeWithType()
        def encoded2 = xWalV2.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def xWalV11 = XWalV.decodeFrom(buffer)
        def buffer2 = ByteBuffer.wrap(encoded2)
        buffer2.get()
        def xWalV22 = XWalV.decodeFrom(buffer2)
        then:
        xWalV11.encodedLength() == encoded.length
        xWalV11.v.encode() == v.encode()
        xWalV11.isValueShort() == xWalV.isValueShort()
        xWalV11.offset == xWalV.offset
        xWalV22.encodedLength() == encoded2.length
        xWalV22.v.encode() == v.encode()
        xWalV22.isValueShort() == xWalV2.isValueShort()
        xWalV22.offset == xWalV2.offset

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            XWalV.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 1 + 4 + 8 + 4 + 8 + 8, (CompressedValue.KEY_MAX_LENGTH + 1).shortValue())
        buffer.position(1)
        try {
            XWalV.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 1 + 4 + 8 + 4 + 8 + 8, (short) 0)
        buffer.position(1)
        try {
            XWalV.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        final byte slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def replPair = ReplPairTest.mockAsSlave()
        xWalV.apply(slot, replPair)
        then:
        localPersist.oneSlot(slot).get(v.key().bytes, v.bucketIndex(), v.keyHash()) != null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
