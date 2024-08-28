package redis

import com.github.luben.zstd.Zstd
import io.activej.config.Config
import io.activej.net.socket.tcp.ITcpSocket
import org.apache.commons.io.FileUtils
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

    final byte slot = 0
    final int slotNumber = 1

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

        println new BaseCommand.SlotWithKeyHashWithKeyBytes(s5, k5.bytes)

        expect:
        s1.slot() == 0
        s1.bucketIndex() < 16384
        s1.keyHash() != 0

        s3.slot() == s4.slot()
    }

    def 'test init'() {
        given:
        def data2 = new byte[2][0]
        data2[0] = 'get'.bytes
        data2[1] = 'key'.bytes
        def c = new SubCommand('get', data2, null)
        c.crossRequestWorker = false
        c.slotWithKeyHashListParsed = null
        c.cmd = 'get'
        c.data = data2
        c.socket = null
        c.localTest = false
        c.localTestRandomValueList = null

        expect:
        c.cmd == 'get'
        c.data == data2
        c.socket == null

        when:
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
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
        c.slot('key3{x}'.bytes).slot() == BaseCommand.slot('key3{x}'.bytes, 1).slot()

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
        def data2 = new byte[2][0]
        data2[0] = 'get'.bytes
        data2[1] = 'key'.bytes

        def c = new SubCommand('get', data2, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
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

//        when:
//        oneSlot.getBigStringDir().listFiles().each {
//            it.delete()
//        }
//        then:
//        c.getCv(bigStringKey.bytes, sBigString).compressedData == null

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
        def longStringBytes = ('aaaaabbbbbccccc' * 10).bytes
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
        valueBytes.length == cv.compressedLength

        when:
        valueBytes = c.get(key.bytes, sKey)
        then:
        valueBytes.length == cv.compressedLength
        c.get('not-exist-key'.bytes, sKey) == null

        when:
        c.byPassGetSet = inMemoryGetSet
        def keyForTypeList = 'key-list'
        def sKeyForTypeList = BaseCommand.slot(keyForTypeList.bytes, slotNumber)
        def cvForTypeList = new CompressedValue()
        cvForTypeList.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvForTypeList.compressedData = new RedisList().encode()
        cvForTypeList.compressedLength = cvForTypeList.compressedLength
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
        cvForTypeList.dictSeqOrSpType = 0
        inMemoryGetSet.put(slot, keyForTypeList, sKeyForTypeList.bucketIndex(), cvForTypeList)
        then:
        c.get(keyForTypeList.bytes, sKeyForTypeList, true) != null
        c.get(keyForTypeList.bytes, sKeyForTypeList, true, 0) != null

        when:
        exception = false
        try {
            c.get(keyForTypeList.bytes, sKeyForTypeList, true, CompressedValue.SP_TYPE_HASH, CompressedValue.SP_TYPE_SET)
        } catch (TypeMismatchException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test set'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def data3 = new byte[3][0]
        data3[0] = 'set'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'value'.bytes

        def c = new SubCommand('set', data3, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, snowFlake, Config.create())
        c.init(requestHandler, new Request(data3, false, false))

        when:
        def key = 'key'
        def sKey = BaseCommand.slot(key.bytes, slotNumber)
        c.byPassGetSet = inMemoryGetSet
        c.setNumber(key.bytes, (short) 1, sKey)
        then:
        c.get(key.bytes, sKey).length == 1

        when:
        c.setNumber(key.bytes, (short) 200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 3

        when:
        c.setNumber(key.bytes, (short) -200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 4

        when:
        c.setNumber(key.bytes, (int) 1, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 1

        when:
        c.setNumber(key.bytes, (int) -200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 4

        when:
        c.setNumber(key.bytes, (int) 65536, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.setNumber(key.bytes, (int) -65537, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 6

        when:
        c.setNumber(key.bytes, (long) 1, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 1

        when:
        c.setNumber(key.bytes, (long) -200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 4

        when:
        c.setNumber(key.bytes, (long) 65536, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.setNumber(key.bytes, (long) -65537, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 6

        when:
        c.setNumber(key.bytes, (long) (1L + Integer.MAX_VALUE), sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 10

        when:
        c.setNumber(key.bytes, (long) (-1L + Integer.MIN_VALUE), sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 11

        when:
        c.setNumber(key.bytes, (double) 0.99, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(key.bytes, sKey).length == 4

        when:
        boolean exception = false
        try {
            c.setNumber(key.bytes, (float) 0.99, sKey, CompressedValue.NO_EXPIRE)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def value = 'value'
        c.set(key.bytes, value.bytes)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.byPassGetSet = null
        c.set(key.bytes, value.bytes)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, value.bytes, sKey)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.byPassGetSet = null
        c.set(key.bytes, value.bytes, sKey)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, value.bytes, sKey, CompressedValue.SP_TYPE_SHORT_STRING)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.byPassGetSet = null
        c.set(key.bytes, value.bytes, sKey, CompressedValue.SP_TYPE_SHORT_STRING)
        then:
        c.get(key.bytes, sKey).length == 5

        when:
        c.byPassGetSet = inMemoryGetSet
        def cv = new CompressedValue()
        cv.compressedData = value.bytes
        cv.compressedLength = value.length()
        cv.uncompressedLength = value.length()
        cv.keyHash = sKey.keyHash()
        cv.expireAt = CompressedValue.NO_EXPIRE
        c.setCv(key.bytes, cv, null)
        c.setCv(key.bytes, cv, sKey)
        then:
        // cv seq is new after set
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().encodedLength() == cv.encodedLength()

        when:
        c.byPassGetSet = null
        c.setCv(key.bytes, cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().encodedLength() == cv.encodedLength()

        when:
        def longValueBytes = ('aaaaabbbbbccccc' * 10).bytes
        c.byPassGetSet = inMemoryGetSet
        cv.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        cv.uncompressedLength = longValueBytes.length
        cv.compressedData = Zstd.compress(longValueBytes, 3)
        cv.compressedLength = cv.compressedData.length
        c.setCv(key.bytes, cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == cv.compressedLength

        when:
        c.byPassGetSet = null
        c.setCv(key.bytes, cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == cv.compressedLength

        when:
        c.byPassGetSet = inMemoryGetSet
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        def intBytes = new byte[4]
        ByteBuffer.wrap(intBytes).putInt(1)
        cv.compressedData = intBytes
        cv.compressedLength = intBytes.length
        cv.uncompressedLength = intBytes.length
        c.setCv(key.bytes, cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        c.byPassGetSet = null
        c.setCv(key.bytes, cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, '1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1234

        when:
        c.byPassGetSet = null
        c.set(key.bytes, '1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1234

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, '0.99'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 0.99

        when:
        c.byPassGetSet = null
        c.set(key.bytes, '0.99'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 0.99

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, intBytes, sKey, CompressedValue.SP_TYPE_NUM_INT, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        c.byPassGetSet = null
        c.set(key.bytes, intBytes, sKey, CompressedValue.SP_TYPE_NUM_INT, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        ConfForGlobal.isValueSetUseCompression = false
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == longValueBytes.length

        when:
        c.byPassGetSet = null
        c.set(key.bytes, longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == longValueBytes.length

        when:
        ConfForGlobal.isValueSetUseCompression = true
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength < longValueBytes.length
        c.remove(slot, sKey.bucketIndex(), key, sKey.keyHash())

        when:
        c.byPassGetSet = null
        c.set(key.bytes, longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        c.getCv(key.bytes, sKey).compressedLength < longValueBytes.length
        c.remove(slot, sKey.bucketIndex(), key, sKey.keyHash())

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(key.bytes, '1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        c.exists(slot, sKey.bucketIndex(), key, sKey.keyHash())
        !c.exists(slot, sKey.bucketIndex(), 'no-exist-key', sKey.keyHash())

        when:
        c.removeDelay(slot, sKey.bucketIndex(), key, sKey.keyHash())
        then:
        inMemoryGetSet.getBuf(slot, key.bytes, sKey.bucketIndex(), sKey.keyHash()) == null

        when:
        c.byPassGetSet = null
        c.set(key.bytes, '1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        c.exists(slot, sKey.bucketIndex(), key, sKey.keyHash())

        when:
        c.removeDelay(slot, sKey.bucketIndex(), key, sKey.keyHash())
        then:
        c.getCv(key.bytes, sKey) == null
        !c.exists(slot, sKey.bucketIndex(), key, sKey.keyHash())

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test train dict'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def data3 = new byte[3][0]
        data3[0] = 'set'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'value'.bytes

        def c = new SubCommand('set', data3, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, snowFlake, Config.create())
        c.init(requestHandler, new Request(data3, false, false))

        and:
        FileUtils.forceMkdir(Consts.testDir)

        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)
        if (dictMap.dictSize() != 0) {
            dictMap.clearAll()
        }

        expect:
        c.handleTrainSampleResult(null) == null

        when:
        def trainSampleResult = new TrainSampleJob.TrainSampleResult(new HashMap<String, Dict>(), new ArrayList<Long>())
        c.handleTrainSampleResult(trainSampleResult)
        then:
        dictMap.dictSize() == 0

        when:
        c.byPassGetSet = inMemoryGetSet
        def longValueBytes = ('aaaaabbbbbccccc' * 10).bytes
        List<String> keyList = []
        1001.times {
            def key = 'key:' + it.toString().padLeft(12, '0')
            keyList << key
            c.set(key.bytes, longValueBytes)
        }
        then:
        dictMap.dictSize() == 1
        dictMap.getDict('key:') != null

        when:
        def firstKey = keyList[0]
        def sFirstKey = BaseCommand.slot(firstKey.bytes, slotNumber)
        // use trained dict
        c.set(firstKey.bytes, longValueBytes, sFirstKey)
        def cvGet = inMemoryGetSet.getBuf(slot, firstKey.bytes, sFirstKey.bucketIndex(), sFirstKey.keyHash()).cv()
        then:
        c.getValueBytesByCv(cvGet).length == longValueBytes.length

        when:
        def trainedDict = dictMap.getDict('key:')
        dictMap.clearAll()
        boolean exception = false
        try {
            c.getValueBytesByCv(cvGet)
        } catch (DictMissingException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        Dict.GLOBAL_ZSTD_DICT.dictBytes = trainedDict.dictBytes
        c.set(firstKey.bytes, longValueBytes)
        cvGet = inMemoryGetSet.getBuf(slot, firstKey.bytes, sFirstKey.bucketIndex(), sFirstKey.keyHash()).cv()
        then:
        cvGet.dictSeqOrSpType == Dict.GLOBAL_ZSTD_DICT_SEQ
        c.getValueBytesByCv(cvGet).length == longValueBytes.length

        when:
        Dict.GLOBAL_ZSTD_DICT.dictBytes = null
        exception = false
        try {
            c.getValueBytesByCv(cvGet)
        } catch (DictMissingException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        dictMap.cleanUp()
        Consts.testDir.deleteDir()
    }
}
