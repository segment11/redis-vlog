package redis.command

import redis.BaseCommand
import redis.mock.InMemoryGetSet
import redis.reply.ErrorReply
import redis.reply.IntegerReply
import redis.reply.NilReply
import redis.reply.OKReply
import spock.lang.Specification

class PGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        def data4 = new byte[4][]

        and:
        data2[1] = 'a'.bytes

        data4[1] = 'a'.bytes

        when:
        def sPexpireList = PGroup.parseSlots('pexpire', data4, slotNumber)
        def sPexpireat = PGroup.parseSlot('pexpireat', data4, slotNumber)
        def sPexpiretime = PGroup.parseSlot('pexpiretime', data2, slotNumber)
        def sPttl = PGroup.parseSlot('pttl', data2, slotNumber)
        def sPsetex = PGroup.parseSlot('psetex', data4, slotNumber)

        def s = PGroup.parseSlot('pxxx', data2, slotNumber)

        then:
        sPexpireList.size() == 1
        sPexpireat != null
        sPexpiretime != null
        sPttl != null
        sPsetex != null
        s == null

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        sPexpireat = PGroup.parseSlot('pexpireat', data3, slotNumber)

        then:
        sPexpireat != null

        when:
        // wrong size
        sPexpireList = PGroup.parseSlots('pexpire', data2, slotNumber)

        then:
        sPexpireList[0] == null

        when:
        sPexpireat = PGroup.parseSlot('pexpireat', data2, slotNumber)

        then:
        sPexpireat == null

        when:
        sPexpiretime = PGroup.parseSlot('pexpiretime', data4, slotNumber)

        then:
        sPexpiretime == null

        when:
        sPttl = PGroup.parseSlot('pttl', data4, slotNumber)

        then:
        sPttl == null

        when:
        sPsetex = PGroup.parseSlot('psetex', data2, slotNumber)

        then:
        sPsetex == null
    }

    def 'test handle'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '60000'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pexpire', data3, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        pGroup.slotWithKeyHashListParsed = PGroup.parseSlots('pexpire', data3, pGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.handle()

        then:
        reply == IntegerReply.REPLY_0

        when:
        pGroup.cmd = 'pexpireat'
        reply = pGroup.handle()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        pGroup.cmd = 'pexpiretime'
        pGroup.data = data2
        pGroup.slotWithKeyHashListParsed = PGroup.parseSlots('pexpiretime', data2, pGroup.slotNumber)

        reply = pGroup.handle()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -2

        when:
        pGroup.cmd = 'pttl'
        reply = pGroup.handle()

        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == -2

        when:
        pGroup.cmd = 'psetex'
        reply = pGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '60000'.bytes
        data4[3] = 'value'.bytes

        pGroup.cmd = 'psetex'
        pGroup.data = data4
        pGroup.slotWithKeyHashListParsed = PGroup.parseSlots('psetex', data4, pGroup.slotNumber)

        reply = pGroup.handle()

        then:
        reply == OKReply.INSTANCE

        when:
        pGroup.cmd = 'pxxx'
        reply = pGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
