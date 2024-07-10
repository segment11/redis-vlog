package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class WGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = WGroup.parseSlots('wx', data, slotNumber)
        def sX = WGroup.parseSlot('wx', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def wGroup = new WGroup('incr', data, null)
        wGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = wGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
