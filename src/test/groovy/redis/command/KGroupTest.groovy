package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import spock.lang.Specification

class KGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sList = KGroup.parseSlots('kx', data, slotNumber)
        def sX = KGroup.parseSlot('kx', data, slotNumber)

        then:
        sList.size() == 1
        sX == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def kGroup = new KGroup('incr', data, null)
        kGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = kGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
