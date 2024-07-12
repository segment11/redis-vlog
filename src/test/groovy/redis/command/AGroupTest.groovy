package redis.command

import redis.BaseCommand
import redis.mock.InMemoryGetSet
import redis.reply.ErrorReply
import redis.reply.IntegerReply
import redis.reply.NilReply
import spock.lang.Specification

class AGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes

        when:
        def slotWithKeyHash = AGroup.parseSlot('append', data3, slotNumber)
        def slotWithKeyHashList = AGroup.parseSlots('append', data3, slotNumber)

        then:
        slotWithKeyHash.slot() == 63
        slotWithKeyHashList.size() == 1

        when:
        slotWithKeyHash = AGroup.parseSlot('appendx', data3, slotNumber)

        then:
        slotWithKeyHash == null

        when:
        def data2 = new byte[2][]
        slotWithKeyHash = AGroup.parseSlot('append', data2, slotNumber)

        then:
        slotWithKeyHash == null
    }

    def 'test handle'() {
        given:
        def data3 = new byte[3][]

        def inMemoryGetSet = new InMemoryGetSet()

        def aGroup1 = new AGroup('append', data3, null)
        aGroup1.byPassGetSet = inMemoryGetSet
        aGroup1.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        data3[1] = 'a'.bytes
        data3[2] = '123'.bytes

        def reply = aGroup1.handle()

        then:
        reply instanceof IntegerReply

        when:
        def aGroupNotCmdMatch = new AGroup('appendx', data3, null)

        reply = aGroupNotCmdMatch.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test append'() {
        given:
        def data3 = new byte[3][]

        and:
        data3[1] = 'a'.bytes
        data3[2] = '123'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def aGroup = new AGroup('append', data3, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        aGroup.append()

        then:
        aGroup.get('a'.bytes) == '123'.bytes

        when:
        data3[2] = '456'.bytes
        aGroup.append()

        then:
        aGroup.get('a'.bytes) == '123456'.bytes

        when:
        def data2 = new byte[2][]
        def aGroup2 = new AGroup('append', data2, null)
//        aGroup2.byPassGetSet = inMemoryGetSet
//        aGroup2.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        def reply = aGroup2.append()

        then:
        reply == ErrorReply.FORMAT
    }
}
