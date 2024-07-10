package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class YGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = YGroup.parseSlots('yx', data, slotNumber)
        def sX = YGroup.parseSlot('yx', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def yGroup = new YGroup('incr', data, null)
        yGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = yGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
