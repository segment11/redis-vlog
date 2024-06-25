package redis.command

import redis.BaseCommand
import redis.reply.NilReply
import redis.reply.OKReply
import spock.lang.Specification

class BGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[3][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def slotWithKeyHash = BGroup.parseSlot('bgsave', data, slotNumber)
        def slotWithKeyHashList = BGroup.parseSlots('bgsave', data, slotNumber)

        then:
        slotWithKeyHash == null
        slotWithKeyHashList.size() == 1
        slotWithKeyHashList[0] == null

        when:
        slotWithKeyHash = BGroup.parseSlot('bgsavex', data, slotNumber)

        then:
        slotWithKeyHash == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[3][]
        def bGroup = new BGroup('bgsavex', data, null)
        bGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = bGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test bgsave'() {
        given:
        byte[][] data = new byte[3][]
        def bGroup = new BGroup('bgsave', data, null)
        bGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = bGroup.handle()

        then:
        reply == OKReply.INSTANCE
    }
}
