package redis.command

import com.github.luben.zstd.Zstd
import redis.BaseCommand
import redis.CompressedValue
import redis.mock.InMemoryGetSet
import redis.persist.Mock
import redis.reply.*
import redis.type.RedisHashKeys
import spock.lang.Specification

class SGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data4 = new byte[4][]
        int slotNumber = 128

        and:
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        when:
        def sSetList = SGroup.parseSlots('set', data4, slotNumber)
        def sSintercardList = SGroup.parseSlots('sintercard', data4, slotNumber)
        def sSmoveList = SGroup.parseSlots('smove', data4, slotNumber)
        def sList = SGroup.parseSlots('sxxx', data4, slotNumber)

        then:
        sSetList.size() == 1
        sSintercardList.size() == 2
        sSmoveList.size() == 2
        sList.size() == 0

        when:
        def sListList = '''
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
'''.readLines().collect { it.trim() }.findAll { it }.collect {
            SGroup.parseSlots(it, data4, slotNumber)
        }

        then:
        sListList.size() == 13
        sListList.every { it.size() == 1 }

        when:
        def sListList2 = '''
sdiff
sdiffstore
sinter
sinterstore
sunion
sunionstore
'''.readLines().collect { it.trim() }.findAll { it }.collect {
            SGroup.parseSlots(it, data4, slotNumber)
        }

        then:
        sListList2.size() == 6
        sListList2.every { it.size() > 1 }

        when:
        def data1 = new byte[1][]

        sSetList = SGroup.parseSlots('set', data1, slotNumber)
        def sDiffList = SGroup.parseSlots('sdiff', data1, slotNumber)
        sSintercardList = SGroup.parseSlots('sintercard', data1, slotNumber)
        sSmoveList = SGroup.parseSlots('smove', data1, slotNumber)

        then:
        sSetList.size() == 0
        sDiffList.size() == 0
        sSintercardList.size() == 0
        sSmoveList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def sGroup = new SGroup('set', data1, null)
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = sGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        def replyList = '''
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
sdiff
sdiffstore
sinter
sinterstore
sunion
sunionstore
sintercard
smove
select
slaveof
'''.readLines().collect { it.trim() }.findAll { it }.collect {
            sGroup.cmd = it
            sGroup.handle()
        }

        then:
        replyList.every { it == ErrorReply.FORMAT }

        when:
        sGroup.cmd = 'save'
        reply = sGroup.handle()

        then:
        reply == OKReply.INSTANCE

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
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

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
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

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
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

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
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('select', data2, sGroup.slotNumber)
        def reply = sGroup.select()

        then:
        reply == ErrorReply.NOT_SUPPORT

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
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        sGroup.slotWithKeyHashListParsed = SGroup.parseSlots('sadd', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.sadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.sadd()

        then:
        reply == ErrorReply.WRONG_TYPE

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
}
