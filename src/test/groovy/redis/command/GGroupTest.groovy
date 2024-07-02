package redis.command

import redis.BaseCommand
import redis.CompressedValue
import redis.mock.InMemoryGetSet
import redis.persist.Mock
import redis.reply.BulkReply
import redis.reply.ErrorReply
import redis.reply.NilReply
import spock.lang.Specification

class GGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sGetList = GGroup.parseSlots('get', data, slotNumber)
        def sGetDel = GGroup.parseSlot('getdel', data, slotNumber)
        def sGetEx = GGroup.parseSlot('getex', data, slotNumber)
        def sGetRange = GGroup.parseSlot('getrange', data, slotNumber)
        def sGetSet = GGroup.parseSlot('getset', data, slotNumber)
        def s = GGroup.parseSlot('gxxx', data, slotNumber)

        then:
        sGetList.size() == 1
        sGetList[0] != null
        sGetDel != null
        sGetEx != null
        sGetRange != null
        sGetSet != null
        s == null

        when:
        data = new byte[1][]

        sGetDel = GGroup.parseSlot('getdel', data, slotNumber)

        then:
        sGetDel == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def gGroup = new GGroup('getdel', data, null)
        gGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = gGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getex'
        reply = gGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getrange'
        reply = gGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getset'
        reply = gGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'zzz'
        reply = gGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test getdel'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getdel', data, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getdel', data, gGroup.slotNumber)
        def reply = gGroup.handle()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)

        reply = gGroup.handle()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cv.compressedData
    }

    def 'test getex'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getex', data, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getex', data, gGroup.slotNumber)
        def reply = gGroup.getex()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)

        reply = gGroup.getex()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cv.compressedData

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = gGroup.getex()

        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'persist'.bytes

        gGroup.data = data

        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        reply = gGroup.getex()

        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)

        then:
        bufOrCv.cv.expireAt == CompressedValue.NO_EXPIRE

        when:
        data[2] = 'persist_'.bytes

        reply = gGroup.getex()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = 'ex'.bytes
        data[3] = '60'.bytes

        gGroup.data = data

        reply = gGroup.getex()

        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)

        then:
        bufOrCv.cv.expireAt > System.currentTimeMillis()

        when:
        data[3] = 'a'.bytes

        reply = gGroup.getex()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[3] = '-1'.bytes

        reply = gGroup.getex()

        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data[2] = 'px'.bytes
        data[3] = '60000'.bytes

        reply = gGroup.getex()

        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)

        then:
        bufOrCv.cv.expireAt > System.currentTimeMillis()

        when:
        data[2] = 'pxat'.bytes
        data[3] = (System.currentTimeMillis() + 1000 * 60).toString().bytes

        reply = gGroup.getex()

        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)

        then:
        bufOrCv.cv.expireAt.toString().bytes == data[3]

        when:
        data[2] = 'exat'.bytes
        data[3] = ((System.currentTimeMillis() / 1000).intValue() + 60).toString().bytes

        reply = gGroup.getex()

        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)

        then:
        bufOrCv.cv.expireAt > System.currentTimeMillis()

        when:
        data[2] = 'xx'.bytes

        reply = gGroup.getex()

        then:
        reply == ErrorReply.SYNTAX

        when:
        data = new byte[5][]
        data[1] = 'a'.bytes
        data[2] = 'ex'.bytes
        data[3] = '60'.bytes
        data[4] = 'xx'.bytes

        gGroup.data = data

        reply = gGroup.getex()

        then:
        reply == ErrorReply.FORMAT
    }

    def 'test getrange'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = '0'.bytes
        data[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getrange', data, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getrange', data, gGroup.slotNumber)
        def reply = gGroup.getrange()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'abc'.bytes
        cv.compressedLength = 3
        cv.uncompressedLength = 3

        inMemoryGetSet.put(slot, 'a', 0, cv)

        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'ab'.bytes

        when:
        data[3] = 'a'.bytes
        reply = gGroup.getrange()

        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data[3] = '-1'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes

        when:
        // empty bytes
        data[3] = '-4'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0

        when:
        data[3] = '3'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes

        when:
        data[2] = '2'.bytes
        data[3] = '1'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0

        when:
        data[2] = '3'.bytes
        data[3] = '-1'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0

        when:
        data[2] = '-2'.bytes
        data[3] = '2'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'bc'.bytes

        when:
        data[2] = '-4'.bytes
        reply = gGroup.getrange()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes
    }

    def 'test getset'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getset', data, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getset', data, gGroup.slotNumber)
        def reply = gGroup.getset()

        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'abc'.bytes
        cv.compressedLength = 3
        cv.uncompressedLength = 3

        inMemoryGetSet.put(slot, 'a', 0, cv)

        reply = gGroup.getset()

        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes

        when:
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)

        then:
        bufOrCv.cv.compressedData == 'value'.bytes
    }
}
