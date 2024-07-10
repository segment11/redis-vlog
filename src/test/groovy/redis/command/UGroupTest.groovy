package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class UGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = UGroup.parseSlots('ux', data, slotNumber)
        def sX = UGroup.parseSlot('ux', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def uGroup = new UGroup('incr', data, null)
        uGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = uGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
