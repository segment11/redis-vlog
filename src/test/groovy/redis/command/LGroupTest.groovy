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

//        when:
//        lGroup.cmd = 'load-rdb'
//        reply = lGroup.handle()
//
//        then:
//        reply == ErrorReply.FORMAT

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
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lindex', data, lGroup.slotNumber)
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
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('linsert', data, lGroup.slotNumber)
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
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('llen', data, lGroup.slotNumber)
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
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lmove', data, lGroup.slotNumber)
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
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lpop', data, lGroup.slotNumber)
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

    def 'test lpos'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lpos', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lpos', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lpos()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpos()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data[2] = 'b'.bytes
        reply = lGroup.lpos()

        then:
        reply == NilReply.INSTANCE

        when:
        def data9 = new byte[9][]
        data9[1] = 'a'.bytes
        data9[2] = 'a'.bytes
        data9[3] = 'rank'.bytes
        data9[4] = '-1'.bytes
        data9[5] = 'count'.bytes
        data9[6] = '1'.bytes
        data9[7] = 'maxlen'.bytes
        data9[8] = '0'.bytes

        lGroup.data = data9
        reply = lGroup.lpos()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rl.removeFirst()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)

        // member
        data9[2] = '5'.bytes
        reply = lGroup.lpos()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 15

        when:
        // rank
        data9[4] = '2'.bytes
        reply = lGroup.lpos()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 15

        when:
        // maxlen
        data9[8] = '10'.bytes
        reply = lGroup.lpos()

        then:
        reply == NilReply.INSTANCE

        when:
        // count
        data9[6] = '2'.bytes
        reply = lGroup.lpos()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        // rank
        data9[4] = '1'.bytes
        // maxlen
        data9[8] = '0'.bytes
        reply = lGroup.lpos()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[0]).integer == 5
        ((MultiBulkReply) reply).replies[1] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[1]).integer == 15

        when:
        // rank
        data9[4] = '-1'.bytes
        reply = lGroup.lpos()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[0]).integer == 15
        ((MultiBulkReply) reply).replies[1] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[1]).integer == 5

        when:
        // count
        data9[6] = '0'.bytes
        reply = lGroup.lpos()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'rank'.bytes
        lGroup.data = data4
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = 'count'.bytes
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = 'maxlen'.bytes
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'rank'.bytes
        data5[4] = 'a'.bytes
        lGroup.data = data5
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[3] = 'count'.bytes
        data5[4] = 'a'.bytes
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[3] = 'maxlen'.bytes
        data5[4] = 'a'.bytes
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[4] = '-1'.bytes
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[3] = 'count'.bytes
        data5[4] = '-1'.bytes
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data5[1] = 'a'.bytes
        data5[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lpos()

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lpush'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lpush', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lpush', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lpush(true, true)

        then:
        reply == IntegerReply.REPLY_0;

        when:
        reply = lGroup.lpush(true, false)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = lGroup.lpush(false, false)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        RedisList.LIST_MAX_SIZE.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpush(true, false)

        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST_COMPRESSED
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        100.times {
            rl.addFirst(('aaaaabbbbcccc' * 5).bytes)
        }
        def encoded = rl.encode()
        def compressedBytes = Zstd.compress(encoded)
        cv.uncompressedLength = encoded.length
        cv.compressedData = compressedBytes

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpush(false, false)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 101

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lpush(true, false)

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data[1] = 'a'.bytes
        data[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lpush(true, false)

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lrange'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = '0'.bytes
        data[3] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lrange', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lrange', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lrange()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lrange()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 3

        when:
        data[2] = '10'.bytes
        reply = lGroup.lrange()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        data[2] = '1'.bytes
        data[3] = '0'.bytes
        reply = lGroup.lrange()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        data[2] = '8'.bytes
        data[3] = '10'.bytes
        reply = lGroup.lrange()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        data[2] = '-2'.bytes
        data[3] = '-1'.bytes
        reply = lGroup.lrange()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        data[2] = '-12'.bytes
        data[3] = '1'.bytes
        reply = lGroup.lrange()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        data[2] = '-12'.bytes
        data[3] = '-13'.bytes
        reply = lGroup.lrange()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lrange()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[2] = 'a'.bytes
        reply = lGroup.lrange()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lrange()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lrem'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = '1'.bytes
        data[3] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lrem', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lrem', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lrem()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lrem()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data[2] = '-1'.bytes
        reply = lGroup.lrem()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = lGroup.lrem()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data[2] = '0'.bytes
        data[3] = '1'.bytes
        reply = lGroup.lrem()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data[2] = '3'.bytes
        data[3] = '2'.bytes
        reply = lGroup.lrem()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST_COMPRESSED
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        100.times {
            rl.addFirst(('aaaaabbbbcccc' * 5).bytes)
        }
        def encoded = rl.encode()
        def compressedBytes = Zstd.compress(encoded)
        cv.uncompressedLength = encoded.length
        cv.compressedData = compressedBytes

        inMemoryGetSet.put(slot, 'a', 0, cv)

        data[2] = '1'.bytes
        data[3] = ('aaaaabbbbcccc' * 5).bytes
        reply = lGroup.lrem()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lrem()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[2] = 'a'.bytes
        reply = lGroup.lrem()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lrem()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data[1] = 'a'.bytes
        data[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lrem()

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lset'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = '1'.bytes
        data[3] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lset', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('lset', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lset()

        then:
        reply == ErrorReply.NO_SUCH_KEY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lset()

        then:
        reply == OKReply.INSTANCE

        when:
        // set again, not change
        reply = lGroup.lset()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '-1'.bytes
        reply = lGroup.lset()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '-11'.bytes
        reply = lGroup.lset()

        then:
        reply == ErrorReply.INDEX_OUT_OF_RANGE

        when:
        data[2] = '10'.bytes
        reply = lGroup.lset()

        then:
        reply == ErrorReply.INDEX_OUT_OF_RANGE

        when:
        data[2] = RedisList.LIST_MAX_SIZE.toString().bytes
        reply = lGroup.lset()

        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST_COMPRESSED
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        100.times {
            rl.addFirst(('aaaaabbbbcccc' * 5).bytes)
        }
        def encoded = rl.encode()
        def compressedBytes = Zstd.compress(encoded)
        cv.uncompressedLength = encoded.length
        cv.compressedData = compressedBytes

        inMemoryGetSet.put(slot, 'a', 0, cv)

        data[2] = '1'.bytes
        data[3] = 'a'.bytes
        reply = lGroup.lset()

        then:
        reply == OKReply.INSTANCE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lset()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[2] = 'a'.bytes
        reply = lGroup.lset()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lset()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data[1] = 'a'.bytes
        data[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lset()

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test ltrim'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = '0'.bytes
        data[3] = '9'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('ltrim', data, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        lGroup.slotWithKeyHashListParsed = LGroup.parseSlots('ltrim', data, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST

        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '-10'.bytes
        data[3] = '-1'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '-11'.bytes
        data[3] = '-1'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '2'.bytes
        data[3] = '3'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '0'.bytes
        data[3] = '-11'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        data[2] = '10'.bytes
        data[3] = '10'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)

        data[2] = '1'.bytes
        data[3] = '0'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST_COMPRESSED
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        100.times {
            rl.addFirst(('aaaaabbbbcccc' * 5).bytes)
        }
        def encoded = rl.encode()
        def compressedBytes = Zstd.compress(encoded)
        cv.uncompressedLength = encoded.length
        cv.compressedData = compressedBytes

        inMemoryGetSet.put(slot, 'a', 0, cv)

        data[2] = '0'.bytes
        data[3] = '9'.bytes
        reply = lGroup.ltrim()

        then:
        reply == OKReply.INSTANCE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.ltrim()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        data[2] = 'a'.bytes
        reply = lGroup.ltrim()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.ltrim()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
