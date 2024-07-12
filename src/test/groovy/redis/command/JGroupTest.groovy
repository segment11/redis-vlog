package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class JGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = JGroup.parseSlots('jx', data2, slotNumber)
        def sX = JGroup.parseSlot('jx', data2, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def jGroup = new JGroup('incr', data1, null)
        jGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = jGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
