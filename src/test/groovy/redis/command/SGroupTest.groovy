package redis.command

import com.github.luben.zstd.Zstd
import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import redis.BaseCommand
import redis.CompressedValue
import redis.SocketInspector
import redis.mock.InMemoryGetSet
import redis.persist.LocalPersist
import redis.persist.Mock
import redis.reply.*
import redis.type.RedisHashKeys
import spock.lang.Specification

import java.nio.channels.SocketChannel
import java.time.Duration

class SGroupTest extends Specification {
    def singleKeyCmdList1 = '''
set
setex
setnx
setrange
strlen
substr
sadd
scard
sismember
smembers
smismember
spop
srandmember
srem
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList2 = '''
sdiff
sdiffstore
sinter
sinterstore
sunion
sunionstore
'''.readLines().collect { it.trim() }.findAll { it }

    final byte slot = 0

    def 'test parse slot'() {
        given:
        def data4 = new byte[4][]
        def data1 = new byte[1][]
        int slotNumber = 128

        and:
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        when:
        LocalPersist.instance.addOneSlotForTest2(slot)
        def sSintercardList = SGroup.parseSlots('sintercard', data4, slotNumber)
        def sSmoveList = SGroup.parseSlots('smove', data4, slotNumber)
        def sSelectList = SGroup.parseSlots('select', data4, slotNumber)
        def sList = SGroup.parseSlots('sxxx', data4, slotNumber)
        then:
        sSintercardList.size() == 2
        sSmoveList.size() == 2
        sSelectList.size() == 1
        sList.size() == 0

        when:
        def sDiffList = SGroup.parseSlots('sdiff', data1, slotNumber)
        sSintercardList = SGroup.parseSlots('sintercard', data1, slotNumber)
        sSmoveList = SGroup.parseSlots('smove', data1, slotNumber)
        then:
        sDiffList.size() == 0
        sSintercardList.size() == 0
        sSmoveList.size() == 0

        when:
        def sListList1 = singleKeyCmdList1.collect {
            SGroup.parseSlots(it, data4, slotNumber)
        }
        then:
        sListList1.size() == 14
        sListList1.every { it.size() == 1 }

        when:
        def sListList11 = singleKeyCmdList1.collect {
            SGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList11.size() == 14
        sListList11.every { it.size() == 0 }

        when:
        def sListList2 = multiKeyCmdList2.collect {
            SGroup.parseSlots(it, data4, slotNumber)
        }
        then:
        sListList2.size() == 6
        sListList2.every { it.size() > 1 }

        when:
        def sListList22 = multiKeyCmdList2.collect {
            SGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList22.size() == 6
        sListList22.every { it.size() == 0 }
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def sGroup = new SGroup('set', data1, null)
        sGroup.from(BaseCommand.mockAGroup())

        def allCmdList = singleKeyCmdList1 + multiKeyCmdList2 + ['sintercard', 'smove', 'select']

        when:
        sGroup.data = data1
        def sAllList = allCmdList.collect {
            sGroup.cmd = it
            sGroup.handle()
        }
        then:
        sAllList.every {
            it == ErrorReply.FORMAT
        }

        when:
        sGroup.cmd = 'save'
        def reply = sGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        sGroup.cmd = 'select'
        reply = sGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        sGroup.cmd = 'zzz'
        reply = sGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test set'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('set', data3, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('set', data3, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.set(data3)
        def slotWithKeyHash = sGroup.slotWithKeyHashListParsed[0]
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a'.bytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.compressedData == 'value'.bytes

        when:
        sGroup.localTest = true
        sGroup.localTestRandomValueList = []
        def rand = new Random()
        100.times {
            def value = new byte[200]
            for (int j = 0; j < value.length; j++) {
                value[j] = (byte) rand.nextInt(Byte.MAX_VALUE + 1)
            }
            sGroup.localTestRandomValueList << value
        }
        data3[1] = new byte[16]
        reply = sGroup.set(data3)
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, data3[1], slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.compressedData[-16..-1] == data3[1]

        when:
        sGroup.localTest = false
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'value'.bytes
        data4[3] = 'nx'.bytes
        sGroup.data = data4
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.set(data4)
        then:
        reply == OKReply.INSTANCE

        when:
        // set nx again
        reply = sGroup.set(data4)
        then:
        reply == NilReply.INSTANCE

        when:
        data4[3] = 'xx'.bytes
        reply = sGroup.set(data4)
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        data4[3] = 'xx'.bytes
        reply = sGroup.set(data4)
        then:
        reply == NilReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        data4[3] = 'keepttl'.bytes
        reply = sGroup.set(data4)
        then:
        reply == OKReply.INSTANCE

        when:
        // keepttl set again
        reply = sGroup.set(data4)
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        data4[3] = 'get'.bytes
        reply = sGroup.set(data4)
        then:
        reply == NilReply.INSTANCE

        when:
        reply = sGroup.set(data4)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'value'.bytes

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[3] = 'get'.bytes
        reply = sGroup.set(data4)
        then:
        reply == ErrorReply.NOT_STRING

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'value'.bytes
        data5[3] = 'ex'.bytes
        data5[4] = '10'.bytes
        reply = sGroup.set(data5)
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, data5[1], slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.expireAt > System.currentTimeMillis() + 9000

        when:
        data5[3] = 'px'.bytes
        data5[4] = '10000'.bytes
        reply = sGroup.set(data5)
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, data5[1], slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.expireAt > System.currentTimeMillis() + 9000

        when:
        data5[3] = 'exat'.bytes
        data5[4] = ((System.currentTimeMillis() / 1000).intValue() + 10).toString().bytes
        reply = sGroup.set(data5)
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, data5[1], slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.expireAt > System.currentTimeMillis() + 9000

        when:
        data5[3] = 'pxat'.bytes
        data5[4] = (System.currentTimeMillis() + 10000).toString().bytes
        reply = sGroup.set(data5)
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, data5[1], slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.expireAt > System.currentTimeMillis() + 9000

        when:
        data5[4] = '-1'.bytes
        reply = sGroup.set(data5)
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, data5[1], slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.expireAt == CompressedValue.NO_EXPIRE

        when:
        data5[4] = 'a'.bytes
        reply = sGroup.set(data5)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        // skip syntax check
        data4[3] = 'zz'.bytes
        reply = sGroup.set(data4)
        then:
        reply == OKReply.INSTANCE

        when:
        data4[3] = 'ex'.bytes
        reply = sGroup.set(data4)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.set(data3)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = sGroup.set(data3)
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test setex'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '10'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('setex', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('setex', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.handle()
        then:
        reply == OKReply.INSTANCE
    }

    def 'test setnx'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('setnx', data3, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('setnx', data3, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.handle()
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = sGroup.handle()
        then:
        reply == IntegerReply.REPLY_0

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.handle()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test setrange'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('setrange', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('setrange', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.setrange()

        def slotWithKeyHash = sGroup.slotWithKeyHashListParsed[0]

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 6
        inMemoryGetSet.getBuf(slot, 'a'.bytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.compressedData[1..-1] == 'value'.bytes

        when:
        reply = sGroup.setrange()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 6

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = '1234567890'.bytes
        cv.compressedLength = 10
        cv.uncompressedLength = 10
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.setrange()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10
        inMemoryGetSet.getBuf(slot, 'a'.bytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.compressedData == '1value7890'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '0'.bytes
        data4[3] = 'value'.bytes
        reply = sGroup.setrange()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10
        inMemoryGetSet.getBuf(slot, 'a'.bytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash)
                .cv.compressedData == 'value67890'.bytes

        when:
        data4[2] = '-1'.bytes
        reply = sGroup.setrange()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data4[2] = 'a'.bytes
        reply = sGroup.setrange()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[2] = '1'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.setrange()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = sGroup.setrange()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test strlen'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('strlen', data2, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('strlen', data2, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.strlen()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = '1234567890'.bytes
        cv.compressedLength = 10
        cv.uncompressedLength = 10

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.strlen()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10
    }

    def 'test select'() {
        given:
        def data2 = new byte[2][]
        data2[1] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('select', data2, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        localPersist.socketInspector = new SocketInspector()
        localPersist.addOneSlotForTest2(slot)
        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)
        sGroup.socketForTest = socket

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('select', data2, sGroup.slotNumber)
        def reply = sGroup.select()
        then:
        reply == OKReply.INSTANCE
        localPersist.socketInspector.getDBSelected(socket) == (byte) 1

        when:
        data2[1] = '-1'.bytes
        reply = sGroup.select()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data2[1] = '16'.bytes
        reply = sGroup.select()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data2[1] = 'a'.bytes
        reply = sGroup.select()
        then:
        reply == ErrorReply.NOT_INTEGER
    }

    def 'test sadd'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sadd', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('sadd', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.sadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        boolean wrongTypeException = false
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        try {
            reply = sGroup.sadd()
        } catch (IllegalStateException e) {
            wrongTypeException = true
        }
        then:
        wrongTypeException

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhk = new RedisHashKeys()
        rhk.add('1')
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.sadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        rhk.remove('1')
        RedisHashKeys.HASH_MAX_SIZE.times {
            rhk.add(it.toString())
        }
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '-1'.bytes
        data4[3] = '-2'.bytes
        reply = sGroup.sadd()
        then:
        reply == ErrorReply.SET_SIZE_TO_LONG

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET_COMPRESSED
        def rhk2 = new RedisHashKeys()
        100.times {
            rhk2.add('aaaaabbbbcccc' * 5 + it.toString())
        }
        def encoded = rhk2.encode()
        def compressedBytes = Zstd.compress(encoded)
        cv.uncompressedLength = encoded.length
        cv.compressedData = compressedBytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '1'.bytes
        data4[3] = '2'.bytes
        reply = sGroup.sadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sadd()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[RedisHashKeys.SET_MEMBER_MAX_LENGTH + 1]
        reply = sGroup.sadd()
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG
    }

    def 'test scard'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('scard', data2, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('scard', data2, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.scard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.scard()
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhk = new RedisHashKeys()
        rhk.add('1')
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.scard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.scard()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test sdiff'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sdiff', data3, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('sdiff', data3, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.sdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        boolean wrongTypeException = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        try {
            reply = sGroup.sdiff(false, false)
        } catch (IllegalStateException e) {
            wrongTypeException = true
        }
        then:
        wrongTypeException

        when:
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        // empty set
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()

        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(false, true)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        rhkA.add('1')
        rhkA.add('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkB = new RedisHashKeys()
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        rhkB.add('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == '2'.bytes

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == '1'.bytes

        when:
        rhkB.remove('1')
        rhkB.add('3')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlotForTest(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        sGroup.isCrossRequestWorker = true
        reply = sGroup.sdiff(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == MultiBulkReply.EMPTY
        }.result

        when:
        reply = sGroup.sdiff(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            (result instanceof MultiBulkReply) && ((MultiBulkReply) result).replies.length == 3
        }.result

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sdiff(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test sdiffstore'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'dst'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sdiffstore', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('sdiffstore', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'dst')
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.sdiffstore(false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        boolean wrongTypeException = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        try {
            reply = sGroup.sdiffstore(false, false)
        } catch (IllegalStateException e) {
            wrongTypeException = true
        }
        then:
        wrongTypeException

        when:
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        // empty set
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.add('1')
        rhkA.add('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkB = new RedisHashKeys()
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rhkB.add('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1
        RedisHashKeys.decode(inMemoryGetSet.getBuf(slot, 'dst'.bytes, 0, 0L).cv.compressedData).contains('2')

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1
        RedisHashKeys.decode(inMemoryGetSet.getBuf(slot, 'dst'.bytes, 0, 0L).cv.compressedData).contains('1')

        when:
        rhkB.remove('1')
        rhkB.add('3')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlotForTest(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        sGroup.isCrossRequestWorker = true
        reply = sGroup.sdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.sdiffstore(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && ((IntegerReply) result).integer == 3
        }.result

        when:
        rhkA.remove('1')
        rhkA.remove('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiffstore(false, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            reply == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.sdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            reply == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.sdiffstore(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && ((IntegerReply) result).integer == 3
        }.result

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.sdiffstore(false, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            reply == IntegerReply.REPLY_0
        }.result

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sdiffstore(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'dst'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sdiffstore(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test sintercard'() {
        given:
        final byte slot = 0

        def data6 = new byte[6][]
        data6[1] = '2'.bytes
        data6[2] = 'a'.bytes
        data6[3] = 'b'.bytes
        data6[4] = 'limit'.bytes
        data6[5] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sintercard', data6, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('sintercard', data6, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.sintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkB = new RedisHashKeys()
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkB.add('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sintercard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data6[5] = '1'.bytes
        reply = sGroup.sintercard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data6[5] = '2'.bytes
        reply = sGroup.sintercard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlotForTest(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        sGroup.isCrossRequestWorker = true
        reply = sGroup.sintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && ((IntegerReply) result).integer == 1
        }.result

        when:
        data6[5] = '1'.bytes
        reply = sGroup.sintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && ((IntegerReply) result).integer == 1
        }.result

        when:
        data6[5] = '0'.bytes
        reply = sGroup.sintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && ((IntegerReply) result).integer == 1
        }.result

        when:
        rhkB.remove('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = sGroup.sintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        data6[3] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sintercard()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data6[1] = 'a'.bytes
        reply = sGroup.sintercard()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data6[1] = '1'.bytes
        reply = sGroup.sintercard()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data6[1] = '2'.bytes
        data6[2] = 'a'.bytes
        data6[3] = 'b'.bytes
        data6[4] = 'limit'.bytes
        data6[5] = 'a'.bytes
        reply = sGroup.sintercard()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data6[4] = 'limitx'.bytes
        reply = sGroup.sintercard()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = '2'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'b'.bytes
        data5[4] = 'limit'.bytes
        sGroup.data = data5
        reply = sGroup.sintercard()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data5[1] = '3'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'b'.bytes
        data5[4] = 'c'.bytes
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.sintercard()
        then:
        reply == IntegerReply.REPLY_0

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test sismember'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sismember', data3, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('sismember', data3, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.sismember()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sismember()
        then:
        reply == IntegerReply.REPLY_1

        when:
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sismember()
        then:
        reply == IntegerReply.REPLY_0

        when:
        data3[2] = new byte[RedisHashKeys.SET_MEMBER_MAX_LENGTH + 1]
        reply = sGroup.sismember()
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        data3[2] = '1'.bytes
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sismember()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test smembers'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('smembers', data2, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('smembers', data2, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.smembers()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smembers()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smembers()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.smembers()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test smismember'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('smismember', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('smismember', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.smismember()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smismember()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == IntegerReply.REPLY_1
        ((MultiBulkReply) reply).replies[1] == IntegerReply.REPLY_0

        when:
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smismember()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[2] = new byte[RedisHashKeys.SET_MEMBER_MAX_LENGTH + 1]
        reply = sGroup.smismember()
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = '1'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.smismember()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test smove'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('smove', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('smove', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.smove()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('11')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smove()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.remove('11')
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smove()
        then:
        reply == IntegerReply.REPLY_1

        when:
        rhkA.add('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smove()
        then:
        reply == IntegerReply.REPLY_1

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlotForTest(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        sGroup.isCrossRequestWorker = true
        rhkA.remove('2')
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        inMemoryGetSet.remove(slot, 'b')
        reply = sGroup.smove()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.smove()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        data4[3] = new byte[RedisHashKeys.SET_MEMBER_MAX_LENGTH + 1]
        reply = sGroup.smove()
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.smove()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.smove()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test srandmember'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('srandmember', data3, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('srandmember', data3, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.srandmember(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        sGroup.data = data2
        reply = sGroup.srandmember(false)
        then:
        reply == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        sGroup.data = data3
        reply = sGroup.srandmember(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        sGroup.data = data2
        reply = sGroup.srandmember(false)
        then:
        reply == NilReply.INSTANCE

        when:
        10.times {
            rhkA.add(it.toString())
        }
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        sGroup.data = data3
        reply = sGroup.srandmember(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        sGroup.data = data2
        reply = sGroup.srandmember(false)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw) as int < 10

        when:
        data3[2] = '11'.bytes
        sGroup.data = data3
        reply = sGroup.srandmember(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 10

        when:
        data3[2] = '-5'.bytes
        reply = sGroup.srandmember(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        data3[2] = '5'.bytes
        reply = sGroup.srandmember(true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        // pop all
        reply = sGroup.srandmember(true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        data3[2] = 'a'.bytes
        sGroup.data = data3
        reply = sGroup.srandmember(false)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.srandmember(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test srem'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('srem', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('srem', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.srem()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.srem()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.srem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        rhkA.add('1')
        rhkA.add('2')
        rhkA.add('3')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.srem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[3] = new byte[RedisHashKeys.SET_MEMBER_MAX_LENGTH + 1]
        reply = sGroup.srem()
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.srem()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
