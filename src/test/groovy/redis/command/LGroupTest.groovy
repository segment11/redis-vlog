package redis.command

import com.github.luben.zstd.Zstd
import redis.BaseCommand
import redis.CompressedValue
import redis.mock.InMemoryGetSet
import redis.persist.Mock
import redis.reply.*
import redis.type.RedisList
import spock.lang.Specification

class LGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sLindexList = LGroup.parseSlots('lindex', data, slotNumber)
        def sLinsert = LGroup.parseSlot('linsert', data, slotNumber)
        def sLlen = LGroup.parseSlot('llen', data, slotNumber)
        def sLpop = LGroup.parseSlot('lpop', data, slotNumber)
        def sLpos = LGroup.parseSlot('lpos', data, slotNumber)
        def sLpush = LGroup.parseSlot('lpush', data, slotNumber)
        def sLpushx = LGroup.parseSlot('lpushx', data, slotNumber)
        def sLrange = LGroup.parseSlot('lrange', data, slotNumber)
        def sLrem = LGroup.parseSlot('lrem', data, slotNumber)
        def sLset = LGroup.parseSlot('lset', data, slotNumber)
        def sLtrim = LGroup.parseSlot('ltrim', data, slotNumber)

        def s = LGroup.parseSlot('lxxx', data, slotNumber)

        then:
        sLindexList.size() == 1
        sLinsert != null
        sLlen != null
        sLpop != null
        sLpos != null
        sLpush != null
        sLpushx != null
        sLrange != null
        sLrem != null
        sLset != null
        sLtrim != null
        s == null

        when:
        data = new byte[1][]

        sLinsert = LGroup.parseSlot('linsert', data, slotNumber)

        then:
        sLinsert == null

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'a'.bytes

        def sLmoveList = LGroup.parseSlots('lmove', data5, slotNumber)

        then:
        sLmoveList.size() == 2

        when:
        // wrong size
        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'a'.bytes

        sLmoveList = LGroup.parseSlots('lmove', data6, slotNumber)

        then:
        sLmoveList.size() == 0
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def lGroup = new LGroup('lindex', data, null)
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'linsert'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'llen'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lmove'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpop'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpos'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpush'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpushx'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lrange'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lrem'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lset'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'ltrim'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'load-rdb'
        reply = lGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'zzz'
        reply = lGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test lindex'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lindex', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = HGroup.parseSlots('lindex', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lindex()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lindex()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        data[2] = '1'.bytes
        reply = lGroup.lindex()

        then:
        reply == NilReply.INSTANCE

        when:
        data[2] = '-1'.bytes
        reply = lGroup.lindex()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        data[2] = '-2'.bytes
        reply = lGroup.lindex()

        then:
        reply == NilReply.INSTANCE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lindex()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[2] = RedisList.LIST_MAX_SIZE.toString().bytes
        reply = lGroup.lindex()

        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        data[2] = 'a'.bytes
        reply = lGroup.lindex()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lindex()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test linsert'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[5][]
        data[1] = 'a'.bytes
        data[2] = 'after'.bytes
        data[3] = 'b'.bytes
        data[4] = 'c'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('linsert', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = HGroup.parseSlots('linsert', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.linsert()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.linsert()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rl = new RedisList()
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)

        data[2] = 'before'.bytes
        reply = lGroup.linsert()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rl.removeFirst()
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.linsert()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.linsert()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[2] = 'xxx'.bytes
        reply = lGroup.linsert()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.linsert()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data[1] = 'a'.bytes
        data[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.linsert()

        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        data[3] = 'b'.bytes
        data[4] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.linsert()

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test llen'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('llen', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = HGroup.parseSlots('llen', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.llen()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.llen()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST_COMPRESSED
        rl.removeLast()
        100.times {
            rl.addFirst(('aaaaabbbbcccc' * 5).bytes)
        }
        def encoded = rl.encode()
        def compressedBytes = Zstd.compress(encoded)
        cv.uncompressedLength = encoded.length
        cv.compressedData = compressedBytes

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.llen()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 100

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.llen()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.llen()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lmove'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[5][]
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes
        data[3] = 'left'.bytes
        data[4] = 'left'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lmove', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = HGroup.parseSlots('lmove', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = lGroup.lmove()

        then:
        reply == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cv = cvList[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)

        def cv1 = cvList[1]
        cv1.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl1 = new RedisList()
        rl1.addFirst('b'.bytes)
        cv1.compressedData = rl1.encode()

        inMemoryGetSet.put(slot, 'b', 0, cv1)
        reply = lGroup.lmove()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)

        data[3] = 'left'.bytes
        data[4] = 'right'.bytes
        reply = lGroup.lmove()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)

        data[3] = 'right'.bytes
        data[4] = 'left'.bytes
        reply = lGroup.lmove()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)

        data[3] = 'right'.bytes
        data[4] = 'right'.bytes
        reply = lGroup.lmove()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lmove()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        inMemoryGetSet.put(slot, 'a', 0, cv)

        cv1.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        reply = lGroup.lmove()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[3] = 'xxx'.bytes
        reply = lGroup.lmove()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data[3] = 'left'.bytes
        data[4] = 'xxx'.bytes
        reply = lGroup.lmove()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lmove()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data[1] = 'a'.bytes
        data[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lmove()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lpop'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lpop', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = HGroup.parseSlots('lpop', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lpop(true)

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpop(true)

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        reply = lGroup.lpop(true)

        then:
        reply == NilReply.INSTANCE

        when:
        rl.removeFirst()
        100.times {
            rl.addFirst(('aaaaabbbbbccccc' * 5).bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '2'.bytes

        lGroup.data = data3
        reply = lGroup.lpop(false)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == ('aaaaabbbbbccccc' * 5).bytes

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpop(true)

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data3[2] = '0'.bytes
        reply = lGroup.lpop(true)

        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data3[2] = 'a'.bytes
        reply = lGroup.lpop(true)

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lpop(true)

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
