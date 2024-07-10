package redis.command

import redis.BaseCommand
import redis.mock.InMemoryGetSet
import redis.reply.ErrorReply
import redis.reply.NilReply
import redis.type.RedisHashKeys
import spock.lang.Specification

class IGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sIncrList = IGroup.parseSlots('incr', data, slotNumber)
        def sIncrby = IGroup.parseSlot('incrby', data, slotNumber)
        def sIncrbyfloat = IGroup.parseSlot('incrbyfloat', data, slotNumber)
        def s = IGroup.parseSlot('ixxx', data, slotNumber)

        then:
        sIncrList.size() == 1
        sIncrby != null
        sIncrbyfloat != null
        s == null

        when:
        data = new byte[1][]

        sIncrby = IGroup.parseSlot('incrby', data, slotNumber)

        then:
        sIncrby == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def iGroup = new IGroup('incr', data, null)
        iGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = iGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'zzz'
        reply = iGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test handle2'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def iGroup = new IGroup('incr', data, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        iGroup.slotWithKeyHashListParsed = IGroup.parseSlots('incr', data, iGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = iGroup.handle()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes

        iGroup.data = data3
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[2] = 'a'.bytes
        reply = iGroup.handle()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[2] = '1.1'.bytes
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()

        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data3[2] = 'a'.bytes
        reply = iGroup.handle()

        then:
        reply == ErrorReply.NOT_FLOAT
    }
}
