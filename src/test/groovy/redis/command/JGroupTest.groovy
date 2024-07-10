package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class JGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = JGroup.parseSlots('jx', data, slotNumber)
        def sX = JGroup.parseSlot('jx', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def jGroup = new JGroup('incr', data, null)
        jGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = jGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
