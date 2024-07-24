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
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sGetList = GGroup.parseSlots('get', data2, slotNumber)
        def sGetDelList = GGroup.parseSlots('getdel', data2, slotNumber)
        def sGetExList = GGroup.parseSlots('getex', data2, slotNumber)
        def sGetRangeList = GGroup.parseSlots('getrange', data2, slotNumber)
        def sGetSetList = GGroup.parseSlots('getset', data2, slotNumber)
        def sList = GGroup.parseSlots('gxxx', data2, slotNumber)
        then:
        sGetList.size() == 1
        sGetDelList.size() == 1
        sGetExList.size() == 1
        sGetRangeList.size() == 1
        sGetSetList.size() == 1
        sList.size() == 0

        when:
        def data1 = new byte[1][]
        sGetList = GGroup.parseSlots('get', data1, slotNumber)
        then:
        sGetList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def gGroup = new GGroup('getdel', data1, null)
        gGroup.from(BaseCommand.mockAGroup())

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

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getdel', data2, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getdel', data2, gGroup.slotNumber)
        def reply = gGroup.getdel()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.getdel()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cv.compressedData
    }

    def 'test getex'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getex', data2, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getex', data2, gGroup.slotNumber)
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
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = gGroup.getex()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'persist'.bytes
        gGroup.data = data3
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.getex()
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv.expireAt == CompressedValue.NO_EXPIRE

        when:
        data3[2] = 'persist_'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'ex'.bytes
        data4[3] = '60'.bytes
        gGroup.data = data4
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv.expireAt > System.currentTimeMillis()

        when:
        data4[3] = 'a'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[3] = '-1'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data4[2] = 'px'.bytes
        data4[3] = '60000'.bytes
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv.expireAt > System.currentTimeMillis()

        when:
        data4[2] = 'pxat'.bytes
        data4[3] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv.expireAt.toString().bytes == data4[3]

        when:
        data4[2] = 'exat'.bytes
        data4[3] = ((System.currentTimeMillis() / 1000).intValue() + 60).toString().bytes
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv.expireAt > System.currentTimeMillis()

        when:
        data4[2] = 'xx'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'ex'.bytes
        data5[3] = '60'.bytes
        data5[4] = 'xx'.bytes
        gGroup.data = data5
        reply = gGroup.getex()
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test getrange'() {
        given:
        final byte slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '0'.bytes
        data4[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getrange', data4, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getrange', data4, gGroup.slotNumber)
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
        data4[3] = 'a'.bytes
        reply = gGroup.getrange()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[3] = '-1'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes

        when:
        // empty bytes
        data4[3] = '-4'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0

        when:
        data4[3] = '3'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes

        when:
        data4[2] = '2'.bytes
        data4[3] = '1'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0

        when:
        data4[2] = '3'.bytes
        data4[3] = '-1'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0

        when:
        data4[2] = '-2'.bytes
        data4[3] = '2'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'bc'.bytes

        when:
        data4[2] = '-4'.bytes
        reply = gGroup.getrange()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes
    }

    def 'test getset'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getset', data3, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        gGroup.slotWithKeyHashListParsed = GGroup.parseSlots('getset', data3, gGroup.slotNumber)
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
