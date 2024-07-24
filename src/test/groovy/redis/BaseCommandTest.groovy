package redis

import com.github.luben.zstd.Zstd
import io.activej.config.Config
import io.activej.net.socket.tcp.ITcpSocket
import redis.decode.Request
import redis.mock.InMemoryGetSet
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.persist.Mock
import redis.reply.Reply
import redis.type.RedisList
import spock.lang.Specification

import java.nio.ByteBuffer

class BaseCommandTest extends Specification {
    static class SubCommand extends BaseCommand {
        SubCommand(String cmd, byte[][] data, ITcpSocket socket) {
            super(cmd, data, socket)
        }

        @Override
        Reply handle() {
            return null
        }
    }

    def 'test static methods'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)

        def k1 = 'key1'
        def s1 = BaseCommand.slot(k1.bytes, 1)
        println s1

        def k11 = 'key11'
        def s11 = BaseCommand.slot(k11.bytes, 2)

        def k2 = 'key2{x'
        def s2 = BaseCommand.slot(k2.bytes, 1)

        def k22 = 'key2}x'
        def s22 = BaseCommand.slot(k22.bytes, 1)

        def k3 = 'key3{x}'
        def s3 = BaseCommand.slot(k3.bytes, 1)

        def k33 = 'key3{x}'
        def s33 = BaseCommand.slot(k33.bytes, 2)

        def k4 = 'key4{x}'
        def s4 = BaseCommand.slot(k4.bytes, 1)

        def k5 = 'key5{xyz}'
        def s5 = BaseCommand.slot(k5.bytes, 1)

        expect:
        s1.slot == 0
        s1.bucketIndex < 16384
        s1.keyHash != 0

        s3.slot == s4.slot
    }

    def 'test init'() {
        given:
        def data2 = new byte[2][0]
        data2[0] = 'get'.bytes
        data2[1] = 'key'.bytes
        def c = new SubCommand('get', data2, null)
        c.crossRequestWorker = false

        expect:
        c.cmd == 'get'
        c.data == data2
        c.socket == null

        when:
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, null, Config.create())
        c.init(requestHandler, new Request(data2, false, false))

        // overwrite
        def aGroup = BaseCommand.mockAGroup()
        def aGroup2 = BaseCommand.mockAGroup()
        aGroup2.byPassGetSet = new InMemoryGetSet()
        c.from(aGroup)
        c.from(aGroup2)
        c.byPassGetSet = null

        then:
        c.workerId == 0
        c.netWorkers == 1
        c.slotNumber == 1
        c.slot('key3{x}').slot() == BaseCommand.slot('key3{x}', 1).slot()

        c.compressStats != null
        c.compressLevel == Zstd.defaultCompressionLevel()
        c.trainSampleListMaxSize == 100
        c.snowFlake != null
        c.trainSampleJob != null
        c.sampleToTrainList.size() == 0

        !c.localTest
        c.localTestRandomValueListSize == 0
        c.localTestRandomValueList.size() == 0

        c.slotWithKeyHashListParsed.size() == 0
        !c.isCrossRequestWorker

        c.handle() == null
    }

    def 'test get'() {
        given:
        final byte slot = 0
        final int slotNumber = 1

        def data2 = new byte[2][0]
        data2[0] = 'get'.bytes
        data2[1] = 'key'.bytes

        def c = new SubCommand('get', data2, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, null, Config.create())
        c.init(requestHandler, new Request(data2, false, false))

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def key = 'key'
        def sKey = BaseCommand.slot(key.bytes, slotNumber)
        c.byPassGetSet = inMemoryGetSet
        then:
        c.getExpireAt(key.bytes, sKey) == null
        c.getCv(key.bytes) == null
        c.getCv(key.bytes, sKey) == null

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.keyHash = sKey.keyHash()
        inMemoryGetSet.put(slot, 'key', sKey.bucketIndex(), cv)
        then:
        c.getExpireAt(key.bytes, sKey) == CompressedValue.NO_EXPIRE

        when:
        c.byPassGetSet = null
        then:
        c.getExpireAt(key.bytes, sKey) == null
        c.getCv(key.bytes, sKey) == null

        when:
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        c.getCv(key.bytes, sKey) != null
        c.getExpireAt(key.bytes, sKey) == CompressedValue.NO_EXPIRE

        when:
        cv.expireAt = System.currentTimeMillis() - 1000
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        c.getCv(key.bytes, sKey) == null

        when:
        // reset no expire
        cv.expireAt = CompressedValue.NO_EXPIRE
        // begin test big string
        def bigStringKey = 'kerry-test-big-string-key'
        def sBigString = BaseCommand.slot(bigStringKey.bytes, slotNumber)
        def cvBigString = Mock.prepareCompressedValueList(1)[0]
        cvBigString.keyHash = sBigString.keyHash()
        def rawData = cvBigString.compressedData
        oneSlot.put(bigStringKey, sBigString.bucketIndex(), cvBigString)
        then:
        // cvBigString compressedData is already changed
        c.getCv(bigStringKey.bytes, sBigString).compressedData == rawData

        when:
        def cvNumber = new CompressedValue()
        cvNumber.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        def intBytes = new byte[4]
        ByteBuffer.wrap(intBytes).putInt(1234)
        cvNumber.compressedData = intBytes
        cvNumber.keyHash = sKey.keyHash()
        def valueBytes = c.getValueBytesByCv(cvNumber)
        then:
        valueBytes == '1234'.bytes

        when:
        def cvString = new CompressedValue()
        cvString.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvString.compressedData = 'hello'.bytes
        valueBytes = c.getValueBytesByCv(cvString)
        then:
        valueBytes == 'hello'.bytes

        when:
        def longStringBytes = ('aaaaabbbbcccc' * 5).bytes
        def cvCompressed = CompressedValue.compress(longStringBytes, Dict.SELF_ZSTD_DICT, 3)
        cvCompressed.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        cvCompressed.keyHash = sKey.keyHash()
        valueBytes = c.getValueBytesByCv(cvCompressed)
        then:
        valueBytes.length == longStringBytes.length

        when:
        c.byPassGetSet = inMemoryGetSet
        inMemoryGetSet.put(slot, 'key', sKey.bucketIndex(), cv)
        valueBytes = c.get(key.bytes)
        then:
        valueBytes.length == cv.compressedData.length

        when:
        valueBytes = c.get(key.bytes, sKey)
        then:
        valueBytes.length == cv.compressedData.length

        when:
        c.byPassGetSet = inMemoryGetSet
        def keyForTypeList = 'key-list'
        def sKeyForTypeList = BaseCommand.slot(keyForTypeList.bytes, slotNumber)
        def cvForTypeList = new CompressedValue()
        cvForTypeList.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvForTypeList.compressedData = new RedisList().encode()
        cvForTypeList.compressedLength = cvForTypeList.compressedData.length
        cvForTypeList.uncompressedLength = cvForTypeList.compressedLength
        cvForTypeList.keyHash = sKeyForTypeList.keyHash()
        inMemoryGetSet.put(slot, keyForTypeList, sKeyForTypeList.bucketIndex(), cvForTypeList)
        boolean exception = false
        try {
            c.get(keyForTypeList.bytes, sKeyForTypeList, true)
        } catch (TypeMismatchException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        c.byPassGetSet = inMemoryGetSet
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
