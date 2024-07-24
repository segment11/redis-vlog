package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class QGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = QGroup.parseSlots('qx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def qGroup = new QGroup('incr', data1, null)
        qGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = qGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }
}
