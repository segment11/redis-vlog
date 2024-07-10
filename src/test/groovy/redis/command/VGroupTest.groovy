package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class VGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = VGroup.parseSlots('vx', data, slotNumber)
        def sX = VGroup.parseSlot('vx', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def vGroup = new VGroup('incr', data, null)
        vGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = vGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
