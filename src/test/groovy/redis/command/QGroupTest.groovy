package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class QGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = QGroup.parseSlots('qx', data, slotNumber)
        def sX = QGroup.parseSlot('qx', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def qGroup = new QGroup('incr', data, null)
        qGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = qGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
