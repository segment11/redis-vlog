package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class OGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = OGroup.parseSlots('ox', data, slotNumber)
        def sX = OGroup.parseSlot('ox', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def oGroup = new OGroup('incr', data, null)
        oGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = oGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
