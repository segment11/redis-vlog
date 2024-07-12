package redis.command

import org.apache.commons.io.FileUtils
import redis.BaseCommand
import redis.CompressedValue
import redis.DictMap
import redis.mock.InMemoryGetSet
import redis.persist.Mock
import redis.reply.*
import redis.type.RedisHashKeys
import spock.lang.Specification

class HGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sHdelList = HGroup.parseSlots('hdel', data2, slotNumber)
        def sHexists = HGroup.parseSlot('hexists', data2, slotNumber)
        def sHget = HGroup.parseSlot('hget', data2, slotNumber)
        def sHgetall = HGroup.parseSlot('hgetall', data2, slotNumber)
        def sHincrby = HGroup.parseSlot('hincrby', data2, slotNumber)
        def sHincrbyfloat = HGroup.parseSlot('hincrbyfloat', data2, slotNumber)
        def sHkeys = HGroup.parseSlot('hkeys', data2, slotNumber)
        def sHlen = HGroup.parseSlot('hlen', data2, slotNumber)
        def sHmget = HGroup.parseSlot('hmget', data2, slotNumber)
        def sHmset = HGroup.parseSlot('hmset', data2, slotNumber)
        def sHrandfield = HGroup.parseSlot('hrandfield', data2, slotNumber)
        def sHset = HGroup.parseSlot('hset', data2, slotNumber)
        def sHsetnx = HGroup.parseSlot('hsetnx', data2, slotNumber)
        def sHstrlen = HGroup.parseSlot('hstrlen', data2, slotNumber)
        def sHvals = HGroup.parseSlot('hvals', data2, slotNumber)
        def sHFieldDictTrain = HGroup.parseSlot('h_field_dict_train', data2, slotNumber)
        def s = HGroup.parseSlot('hxxx', data2, slotNumber)

        then:
        sHdelList.size() == 1
        sHexists != null
        sHget != null
        sHgetall != null
        sHincrby != null
        sHincrbyfloat != null
        sHkeys != null
        sHlen != null
        sHmget != null
        sHmset != null
        sHrandfield != null
        sHset != null
        sHsetnx != null
        sHstrlen != null
        sHvals != null
        sHFieldDictTrain == null
        s == null

        when:
        def data1 = new byte[1][]

        sHget = HGroup.parseSlot('hget', data1, slotNumber)

        then:
        sHget == null
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def hGroup = new HGroup('hdel', data1, null)
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hexists'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hget'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hgetall'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hincrby'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hincrbyfloat'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hkeys'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hlen'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hmget'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hmset'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        def data5 = new byte[5][]
        hGroup.data = data5
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.data = data1
        hGroup.cmd = 'hrandfield'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hset'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hsetnx'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hstrlen'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hvals'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'h_field_dict_train'
        reply = hGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'zzz'
        reply = hGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test hdel'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hdel', data3, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hdel', data3, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hdel()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        def rhk = new RedisHashKeys()
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hdel()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = hGroup.hdel()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rhk.remove('field')
        100.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hdel()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hdel()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hdel()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hexists'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hexists', data3, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hexists', data3, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.hexists()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)

        reply = hGroup.hexists()

        then:
        reply == IntegerReply.REPLY_1

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hexists()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hexists()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hget'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hget', data3, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hget', data3, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.hget(true)

        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = hGroup.hget(false)

        then:
        reply == NilReply.INSTANCE

        when:
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)

        reply = hGroup.hget(true)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == cvField.compressedData.length

        when:
        reply = hGroup.hget(false)

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cvField.compressedData

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hget(true)

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hget(true)

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hgetall'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hgetall', data2, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hgetall', data2, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hgetall()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvKeys = cvList[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        def rhk = new RedisHashKeys()
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()

        def cvField = cvList[1]

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)

        reply = hGroup.hgetall()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == cvField.compressedData

        when:
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        reply = hGroup.hgetall()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof NilReply

        when:
        rhk.remove('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hgetall()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hgetall()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hincrby'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hincrby', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hincrby', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.hincrby(false)

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.hincrby(true)

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[3] = 'a'.bytes
        reply = hGroup.hincrby(false)

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.hincrby(true)

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hincrby(false)

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hincrby(false)

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hkeys'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hkeys', data2, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hkeys', data2, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hkeys(false)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = hGroup.hkeys(true)

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        def rhk = new RedisHashKeys()
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hkeys(false)

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes

        when:
        reply = hGroup.hkeys(true)

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        rhk.remove('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hkeys(false)

        then:
        reply == MultiBulkReply.EMPTY

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hkeys(false)

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hmget'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'field1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hmget', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hmget', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hmget()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvField = cvList[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        def cvField1 = cvList[1]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)

        reply = hGroup.hmget()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == cvField.compressedData
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == cvField1.compressedData

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmget()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmget()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hmset'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hmset', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hmset', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hmset()

        then:
        reply == OKReply.INSTANCE

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        def rhk = new RedisHashKeys()
        RedisHashKeys.HASH_MAX_SIZE.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hmset()

        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        rhk.remove('field0')
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'field'.bytes
        data6[3] = 'value'.bytes
        data6[4] = 'field0'.bytes
        data6[5] = 'value0'.bytes

        hGroup.data = data6
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hmset', data6, hGroup.slotNumber)

        reply = hGroup.hmset()

        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        10.times {
            rhk.remove('field' + it)
        }
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hmset()

        then:
        reply == OKReply.INSTANCE

        // get and compare
        when:
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'field0'.bytes
        hGroup.data = data4
        hGroup.cmd = 'hmget'

        reply = hGroup.hmget()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'value'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == 'value0'.bytes

        when:
        hGroup.data = data4
        hGroup.cmd = 'hmset'
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmset()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmset()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[2] = 'field'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = hGroup.hmset()

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test hrandfield'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hrandfield', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hrandfield', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hrandfield()

        then:
        reply == NilReply.INSTANCE

        when:
        data4[3] = 'withvalues'.bytes
        reply = hGroup.hrandfield()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        data4[3] = '1'.bytes
        reply = hGroup.hrandfield()

        then:
        reply == NilReply.INSTANCE

        when:
        data4[3] = 'withvalues'.bytes
        reply = hGroup.hrandfield()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        10.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        data4[3] = '1'.bytes
        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        // > rhk size
        data4[3] = (rhk.size() + 1).toString().bytes
        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == rhk.size()

        when:
        data4[2] = '1'.bytes
        data4[3] = 'withvalues'.bytes
        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(rhk.size())
        rhk.size().times {
            inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field' + it), 0, cvList[it])
        }

        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply

        when:
        data4[2] = '-1'.bytes

        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply

        when:
        data4[2] = '5'.bytes
        data4[3] = '5'.bytes

        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        data4[2] = '-5'.bytes
        data4[3] = '-5'.bytes

        reply = hGroup.hrandfield()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hrandfield()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = 'a'.bytes
        reply = hGroup.hrandfield()

        then:
        reply == ErrorReply.NOT_INTEGER
    }

    def 'test hsetnx'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hsetnx', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hsetnx', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hsetnx()

        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = hGroup.hsetnx()

        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hsetnx()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hsetnx()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[2] = 'field'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = hGroup.hsetnx()

        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test hvals'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hvals', data2, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('hvals', data2, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hvals()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH

        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hvals()

        then:
        reply == MultiBulkReply.EMPTY

        when:
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()

        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)

        reply = hGroup.hvals()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE

        when:
        var cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)

        reply = hGroup.hvals()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == cvField.compressedData

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hvals()

        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test h_field_dict_train'() {
        given:
        def dirFile = new File('/tmp/redis-vlog-test-dir')
        FileUtils.forceMkdir(dirFile)

        def dictMap = DictMap.instance
        dictMap.initDictMap(dirFile)

        and:
        def data13 = new byte[13][]
        data13[1] = 'key:'.bytes
        11.times {
            data13[it + 2] = ('aaaaabbbbbccccc' * 5).bytes
        }

        def hGroup = new HGroup('h_field_dict_train', data13, null)
        hGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('h_field_dict_train', data13, hGroup.slotNumber)
        def reply = hGroup.h_field_dict_train()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1
        dictMap.dictSize() == 1

        when:
        def data10 = new byte[10][]
        data10[1] = 'key:'.bytes

        hGroup.data = data10
        hGroup.slotWithKeyHashListParsed = HGroup.parseSlots('h_field_dict_train', data10, hGroup.slotNumber)

        reply = hGroup.h_field_dict_train()

        then:
        reply instanceof ErrorReply

        cleanup:
        dictMap.clearAll()
        dictMap.close()
    }
}
