package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import redis.reply.OKReply
import spock.lang.Specification

class BGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes

        when:
        def slotWithKeyHash = BGroup.parseSlot('bgsave', data3, slotNumber)
        def slotWithKeyHashList = BGroup.parseSlots('bgsave', data3, slotNumber)

        then:
        slotWithKeyHash == null
        slotWithKeyHashList.size() == 1
        slotWithKeyHashList[0] == null

        when:
        slotWithKeyHash = BGroup.parseSlot('bgsavex', data3, slotNumber)

        then:
        slotWithKeyHash == null
    }

    def 'test handle'() {
        given:
        def data3 = new byte[3][]
        def bGroup = new BGroup('bgsavex', data3, null)
        bGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = bGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test bgsave'() {
        given:
        def data3 = new byte[3][]
        def bGroup = new BGroup('bgsave', data3, null)
        bGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = bGroup.handle()

        then:
        reply == OKReply.INSTANCE
    }
}
