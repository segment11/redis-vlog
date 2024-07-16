package redis.command

import redis.BaseCommand
import redis.CompressedValue
import redis.mock.InMemoryGetSet
import redis.persist.Mock
import redis.reply.*
import redis.type.RedisZSet
import spock.lang.Specification

class ZGroupTest extends Specification {
    def singleKeyCmdList1 = '''
zadd
zcard
zcount
zincrby
zlexcount
zmscore
zpopmax
zpopmin
zrandmember
zrange
zrangebylex
zrangebyscore
zrank
zrem
zremrangebylex
zremrangebyrank
zremrangebyscore
zrevrange
zrevrangebylex
zrevrangebyscore
zrevrank
zscore
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList2 = '''
zdiff
zinter
zunion
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList3 = '''
zdiffstore
zinterstore
zunionstore
'''.readLines().collect { it.trim() }.findAll { it }

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]
        def data4 = new byte[4][]
        int slotNumber = 128

        and:
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        when:
        def sZdiff = ZGroup.parseSlots('zdiff', data1, slotNumber)
        def sZintercard = ZGroup.parseSlots('zintercard', data1, slotNumber)
        def sList = ZGroup.parseSlots('zxxx', data4, slotNumber)

        then:
        sZdiff.size() == 0
        sZintercard.size() == 0
        sList.size() == 0

        when:
        def sListList1 = singleKeyCmdList1.collect {
            ZGroup.parseSlots(it, data4, slotNumber)
        }
        def sListList11 = singleKeyCmdList1.collect {
            ZGroup.parseSlots(it, data1, slotNumber)
        }

        then:
        sListList1.size() == 22
        sListList1.every { it.size() == 1 }
        sListList11.size() == 22
        sListList11.every { it.size() == 0 }

        when:
        def sListList2 = multiKeyCmdList2.collect {
            ZGroup.parseSlots(it, data4, slotNumber)
        }
        def sListList22 = multiKeyCmdList2.collect {
            ZGroup.parseSlots(it, data1, slotNumber)
        }

        then:
        sListList2.size() == 3
        sListList2.every { it.size() > 1 }
        sListList22.size() == 3
        sListList22.every { it.size() == 0 }

        when:
        def data5 = new byte[5][]
        data5[1] = 'dst'.bytes
        data5[2] = '2'.bytes
        data5[3] = 'a'.bytes
        data5[4] = 'b'.bytes

        def sListList3 = multiKeyCmdList3.collect {
            ZGroup.parseSlots(it, data5, slotNumber)
        }
        def sListList33 = multiKeyCmdList3.collect {
            ZGroup.parseSlots(it, data1, slotNumber)
        }

        then:
        sListList3.size() == 3
        sListList3.every { it.size() == 3 }
        sListList33.size() == 3
        sListList33.every { it.size() == 0 }

        when:
        // zrangestore
        data5[1] = 'dst'.bytes
        data5[2] = 'a'.bytes
        data5[3] = '0'.bytes
        data5[4] = '-1'.bytes

        def sZrangestoreList = ZGroup.parseSlots('zrangestore', data5, slotNumber)

        then:
        sZrangestoreList.size() == 2

        when:
        sZrangestoreList = ZGroup.parseSlots('zrangestore', data1, slotNumber)

        then:
        sZrangestoreList.size() == 0

        when:
        // zintercard
        data4[1] = '2'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes

        def sZintercardList = ZGroup.parseSlots('zintercard', data4, slotNumber)

        then:
        sZintercardList.size() == 2

        when:
        sZintercardList = ZGroup.parseSlots('zintercard', data1, slotNumber)

        then:
        sZintercardList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def zGroup = new ZGroup('zadd', data1, null)
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        def allCmdList = singleKeyCmdList1 + multiKeyCmdList2 + multiKeyCmdList3 + ['zrangestore', 'zintercard']

        when:
        zGroup.data = data1
        def sAllList = allCmdList.collect {
            zGroup.cmd = it
            zGroup.handle()
        }

        then:
        sAllList.every {
            it == ErrorReply.FORMAT
        }

        when:
        zGroup.cmd = 'zzz'
        def reply = zGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    private RedisZSet fromMem(InMemoryGetSet inMemoryGetSet, String key) {
        def buf = inMemoryGetSet.getBuf((byte) 0, key.bytes, 0, 0L)
        RedisZSet.decode(buf.cv().compressedData)
    }

    def 'test zadd'() {
        given:
        final byte slot = 0

        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'member0'.bytes
        data6[4] = '1'.bytes
        data6[5] = 'member1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zadd', data6, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zadd', data6, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 0
        fromMem(inMemoryGetSet, 'a').get('member1').score() == 1

        when:
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data6[2] = 'a'.bytes
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data6[1] = 'a'.bytes
        data6[2] = '0'.bytes
        data6[3] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data6[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'nx'.bytes
        data4[3] = '0'.bytes

        zGroup.data = data4
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.SYNTAX

        when:
        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = 'nx'.bytes
        data7[3] = '0'.bytes
        data7[4] = 'member0'.bytes
        data7[5] = '1'.bytes
        data7[6] = 'member1'.bytes

        zGroup.data = data7
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'xx'.bytes

        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'gt'.bytes

        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'lt'.bytes

        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'incr'.bytes
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'incr'.bytes
        data5[3] = '0'.bytes
        data5[4] = 'member0'.bytes

        zGroup.data = data5
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data5[2] = 'ch'.bytes
        data5[3] = '1'.bytes
        reply = zGroup.zadd()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data5[3] = 'nx'.bytes
        data5[4] = 'lt'.bytes
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.SYNTAX

        when:
        rz.remove('member0')
        rz.remove('member1')

        RedisZSet.ZSET_MAX_SIZE.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)

        data5[2] = 'ch'.bytes
        data5[3] = '0'.bytes
        data5[4] = 'extend_member0'.bytes
        reply = zGroup.zadd()

        then:
        reply == ErrorReply.ZSET_SIZE_TO_LONG
    }

    def 'test zcard'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zcard', data2, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zcard', data2, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zcard()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcard()

        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcard()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zcard()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zcount'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '(1'.bytes
        data4[3] = '(4'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zcount', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zcount', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zcount(false)

        then:
        reply == IntegerReply.REPLY_0

        when:
        boolean wrongTypeException = false

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        inMemoryGetSet.put(slot, 'a', 0, cv)

        try {
            reply = zGroup.zcount(false)
        } catch (IllegalStateException e) {
            wrongTypeException = true
        }

        then:
        wrongTypeException

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcount(false)

        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it, 'member' + it)
        }
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcount(false)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '(member1'.bytes
        data4[3] = '(member4'.bytes
        reply = zGroup.zcount(true)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '[member1'
        data4[3] = '[member4'
        reply = zGroup.zcount(true)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        data4[2] = '2'
        data4[3] = '3'
        reply = zGroup.zcount(false)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '-inf'.bytes
        data4[3] = '+inf'.bytes
        reply = zGroup.zcount(false)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        when:
        data4[2] = '3'
        data4[3] = '2'
        reply = zGroup.zcount(false)

        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '[3'
        data4[3] = '[2'
        reply = zGroup.zcount(false)

        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '[member3'
        data4[3] = '[member2'
        reply = zGroup.zcount(true)

        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zcount(false)

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[2] = '1'.bytes
        data4[3] = 'a'.bytes
        reply = zGroup.zcount(false)

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[2] = 'member1'.bytes
        reply = zGroup.zcount(true)

        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = '[member1'.bytes
        data4[3] = 'member4'.bytes
        reply = zGroup.zcount(true)

        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = '4'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zcount(false)

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zdiff'() {
        // todo
        given:
        final byte slot = 0

        expect:
        1 == 1
    }

    def 'test zdiffstore'() {
        // todo
        given:
        final byte slot = 0

        expect:
        1 == 1
    }

    def 'test zincrby'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = 'member0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zincrby', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zincrby', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zincrby()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '1.0'.bytes

        when:
        reply = zGroup.zincrby()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '2.0'.bytes

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        RedisZSet.ZSET_MAX_SIZE.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[3] = 'extend_member0'.bytes
        reply = zGroup.zincrby()

        then:
        reply == ErrorReply.ZSET_SIZE_TO_LONG

        when:
        data4[3] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zincrby()

        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zincrby()

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zincrby()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zintercard'() {
        // todo
        given:
        final byte slot = 0

        expect:
        1 == 1
    }

    def 'test zmscore'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'member0'.bytes
        data4[3] = 'member1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zmscore', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zmscore', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zmscore()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zmscore()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zmscore()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == '0.1'.bytes
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        data4[2] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zmscore()

        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = 'member0'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zmscore()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zpopmax'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zpopmax', data3, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zpopmax', data3, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zpopmax(false)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zpopmax(false)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        rz.add(100, 'member100')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zpopmax(false)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '100.0'.bytes

        when:
        rz.add(100, 'member100')
        rz.add(10, 'member10')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zpopmax(true)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member10'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '10.0'.bytes

        when:
        data3[2] = '0'.bytes
        reply = zGroup.zpopmax(false)

        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data3[2] = 'a'.bytes
        reply = zGroup.zpopmax(false)

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[2] = '1'.bytes
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zpopmax(false)

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        zGroup.data = data2
        reply = zGroup.zpopmax(false)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
    }

    def 'test zrandmember'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = 'withscores'.bytes

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrandmember', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zrandmember', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zrandmember()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.data = data2
        reply = zGroup.zrandmember()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        zGroup.data = data4
        reply = zGroup.zrandmember()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.data = data2
        reply = zGroup.zrandmember()

        then:
        reply == NilReply.INSTANCE

        when:
        rz.add(100, 'member100')
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        zGroup.data = data4
        reply = zGroup.zrandmember()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '100.0'.bytes

        when:
        zGroup.data = data2
        reply = zGroup.zrandmember()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes

        when:
        data4[2] = '2'.bytes
        zGroup.data = data4
        reply = zGroup.zrandmember()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        rz.remove('member100')
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '5'.bytes
        reply = zGroup.zrandmember()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 10

        when:
        data4[2] = '-3'.bytes
        reply = zGroup.zrandmember()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 6

        when:
        data4[3] = '_withscores'.bytes
        reply = zGroup.zrandmember()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = 'withscores'.bytes
        data4[2] = '0'.bytes
        reply = zGroup.zrandmember()

        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zrandmember()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zrandmember()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'withscores'.bytes

        zGroup.data = data3
        reply = zGroup.zrandmember()

        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test zrange'() {
        given:
        final byte slot = 0

        def data10 = new byte[10][]
        data10[1] = 'a'.bytes
        data10[2] = '(1'.bytes
        data10[3] = '(4'.bytes
        data10[4] = 'byscore'.bytes
        data10[5] = 'rev'.bytes
        data10[6] = 'limit'.bytes
        data10[7] = '0'.bytes
        data10[8] = '0'.bytes
        data10[9] = 'withscores'.bytes

        // no rev first
        data10[5] = 'byscore'.bytes

        def dstKeyBytes = 'dst'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrange', data10, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zrange', data10, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET

        def rz = new RedisZSet()
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // limit count
        data10[8] = '3'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // not withscores
        data10[9] = 'byscore'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit count
        data10[8] = '0'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit offset
        data10[7] = '1'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        // limit offset
        data10[7] = '3'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        // limit offset
        data10[7] = '0'.bytes
        // limit count
        data10[8] = '0'.bytes
        // rev
        data10[5] = 'rev'.bytes
        // start / stop
        data10[2] = '(4'.bytes
        data10[3] = '(1'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        def tmpData5 = new byte[5][]
        tmpData5[1] = dstKeyBytes
        tmpData5[2] = 'a'.bytes

        zGroup.slotWithKeyHashListParsed = ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)

        // rev
        data10[5] = 'byscore'
        // start / stop
        data10[2] = '(1'.bytes
        data10[3] = '(4'.bytes

        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2
        inMemoryGetSet.getBuf(slot, dstKeyBytes, 0, 0L) != null

        when:
        // limit count
        data10[8] = '2'.bytes
        // query result count = 3
        data10[2] = '(1'.bytes
        data10[3] = '(5'.bytes
        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[2] = '1.1'.bytes
        data10[3] = '1.2'.bytes
        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '1.1'.bytes
        data10[3] = '1.2'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '[1.1'.bytes
        data10[3] = '[1.2'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '[1.1'.bytes
        data10[3] = '[1.a'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '[1.a'.bytes
        data10[3] = '[1.2'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        // limit count
        data10[8] = '2'.bytes
        data10[5] = 'byscore'.bytes
        data10[2] = '-inf'.bytes
        data10[3] = '+inf'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[5] = 'byscore'.bytes
        data10[2] = '2'.bytes
        data10[3] = '1'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply == IntegerReply.REPLY_0

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[5] = 'rev'.bytes
        data10[2] = '1'.bytes
        data10[3] = '2'.bytes
        reply = zGroup.zrange(data10)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)

        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zrange(data10)

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
