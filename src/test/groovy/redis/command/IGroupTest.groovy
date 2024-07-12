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
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sIncrList = IGroup.parseSlots('incr', data2, slotNumber)
        def sIncrbyList = IGroup.parseSlots('incrby', data2, slotNumber)
        def sIncrbyfloatList = IGroup.parseSlots('incrbyfloat', data2, slotNumber)
        def sList = IGroup.parseSlots('ixxx', data2, slotNumber)

        then:
        sIncrList.size() == 1
        sIncrbyList.size() == 1
        sIncrbyfloatList.size() == 1
        sList.size() == 0

        when:
        def data1 = new byte[1][]

        sIncrbyList = IGroup.parseSlots('incrby', data1, slotNumber)

        then:
        sIncrbyList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def iGroup = new IGroup('incr', data1, null)
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

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def iGroup = new IGroup('incr', data2, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        iGroup.slotWithKeyHashListParsed = IGroup.parseSlots('incr', data2, iGroup.slotNumber)
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
