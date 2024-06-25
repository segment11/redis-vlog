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
        byte[][] dataAppend = new byte[3][]
        int slotNumber = 128

        and:
        dataAppend[1] = 'a'.bytes

        when:
        def slotWithKeyHash = AGroup.parseSlot('append', dataAppend, slotNumber)
        def slotWithKeyHashList = AGroup.parseSlots('append', dataAppend, slotNumber)

        then:
        slotWithKeyHash.slot() == 63
        slotWithKeyHashList.size() == 1

        when:
        slotWithKeyHash = AGroup.parseSlot('appendx', dataAppend, slotNumber)

        then:
        slotWithKeyHash == null

        when:
        byte[][] dataWrongSize = new byte[2][]
        slotWithKeyHash = AGroup.parseSlot('append', dataWrongSize, slotNumber)

        then:
        slotWithKeyHash == null
    }

    def 'test handle'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        byte[][] data1 = new byte[3][]
        def aGroup1 = new AGroup('append', data1, null)
        aGroup1.byPassGetSet = inMemoryGetSet
        aGroup1.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        data1[1] = 'a'.bytes
        data1[2] = '123'.bytes

        def reply = aGroup1.handle()

        then:
        reply instanceof IntegerReply

        when:
        def aGroupNotCmdMatch = new AGroup('appendx', data1, null)

        reply = aGroupNotCmdMatch.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test append'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        byte[][] dataAppend = new byte[3][]

        and:
        dataAppend[1] = 'a'.bytes
        dataAppend[2] = '123'.bytes

        def aGroup = new AGroup('append', dataAppend, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        aGroup.append()

        then:
        aGroup.get('a'.bytes) == '123'.bytes

        when:
        dataAppend[2] = '456'.bytes
        aGroup.append()

        then:
        aGroup.get('a'.bytes) == '123456'.bytes

        when:
        byte[][] dataWrongSize = new byte[2][]
        def aGroup2 = new AGroup('append', dataWrongSize, null)
//        aGroup2.byPassGetSet = inMemoryGetSet
//        aGroup2.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        def reply = aGroup2.append()

        then:
        reply == ErrorReply.FORMAT
    }
}
