package redis


import io.netty.buffer.Unpooled
import spock.lang.Specification

import java.nio.ByteBuffer

class CompressedValueTest extends Specification {

    def 'test type'() {
        given:
        def cv = new CompressedValue()
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        cv.compressedData = new byte[0]

        expect:
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_BYTE)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_SHORT)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_INT)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_LONG)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_DOUBLE)

        CompressedValue.preferCompress(CompressedValue.SP_TYPE_HH_COMPRESSED)
        CompressedValue.preferCompress(CompressedValue.SP_TYPE_HASH_COMPRESSED)
        CompressedValue.preferCompress(CompressedValue.SP_TYPE_LIST_COMPRESSED)
        CompressedValue.preferCompress(CompressedValue.SP_TYPE_SET_COMPRESSED)
        CompressedValue.preferCompress(CompressedValue.SP_TYPE_ZSET_COMPRESSED)

        CompressedValue.isTypeString(cv.dictSeqOrSpType)
        CompressedValue.isDeleted(new byte[]{CompressedValue.SP_FLAG_DELETE_TMP})

        cv.isTypeNumber()
        cv.isTypeString()
        cv.isShortString()
    }

    def 'test expire'() {
        given:
        def cv = new CompressedValue()

        expect:
        !cv.isExpired()

        when:
        cv.expireAt = System.currentTimeMillis() - 1000

        then:
        cv.isExpired()

        when:
        cv.expireAt = CompressedValue.EXPIRE_NOW

        then:
        cv.isExpired()
    }

    def 'test encode'() {
        given:
        def cv = new CompressedValue()
        cv.seq = 123L
        cv.dictSeqOrSpType = 1
        cv.keyHash = 123L
        cv.compressedData = new byte[10]
        cv.compressedLength = 10
        cv.uncompressedLength = 10

        when:
        def encoded = cv.encode()
        def cvDecode = CompressedValue.decode(Unpooled.wrappedBuffer(encoded), null, 0L, false)

        then:
        cvDecode.seq == cv.seq
        cvDecode.dictSeqOrSpType == cv.dictSeqOrSpType
        cvDecode.keyHash == cv.keyHash
        cvDecode.compressedLength == cv.compressedLength
        cvDecode.uncompressedLength == cv.uncompressedLength
        Arrays.equals(cvDecode.compressedData, cv.compressedData)

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        cv.compressedData[0] = Byte.MAX_VALUE
        def encodedNumber = cv.encodeAsNumber()
        def encodedBufferNumber = ByteBuffer.wrap(encodedNumber)
        def cvDecodeNumber = CompressedValue.decode(Unpooled.wrappedBuffer(encodedNumber), null, 0L, false)

        then:
        encodedNumber.length == 10
        encodedBufferNumber.getLong(1) == cv.seq
        encodedBufferNumber.get(9) == Byte.MAX_VALUE
        cvDecodeNumber.numberValue() == Byte.MAX_VALUE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        def encodedShortString = cv.encodeAsShortString()
        def encodedBufferShortString = ByteBuffer.wrap(encodedShortString)

        then:
        // 1 + 8 seq long + 10 compressed data
        encodedShortString.length == 19
        encodedBufferShortString.getLong(1) == cv.seq
        encodedBufferShortString.slice(9, 10) == ByteBuffer.wrap(cv.compressedData)

        when:
        cv.dictSeqOrSpType = 100
        def encodedBigStringMeta = cv.encodeAsBigStringMeta(890L)
        def cvDecodeBigStringMeta = CompressedValue.decode(Unpooled.wrappedBuffer(encodedBigStringMeta), null, 0L, false)
        def bufferBigStringMeta = ByteBuffer.wrap(cvDecodeBigStringMeta.compressedData)

        then:
        cv.isUseDict()
        cvDecodeBigStringMeta.isBigString()
        bufferBigStringMeta.getLong() == 890L
        bufferBigStringMeta.getInt() == 100
    }
}
