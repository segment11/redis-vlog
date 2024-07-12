package redis.command

import io.activej.eventloop.Eventloop
import redis.BaseCommand
import redis.mock.InMemoryGetSet
import redis.persist.LocalPersist
import redis.persist.Mock
import redis.reply.*
import spock.lang.Specification

import java.time.Duration

class MGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data5 = new byte[5][]
        int slotNumber = 128

        and:
        data5[1] = 'a'.bytes
        data5[2] = '0'.bytes
        data5[3] = '0'.bytes
        data5[4] = '0'.bytes

        when:
        def sMgetList = MGroup.parseSlots('mget', data5, slotNumber)
        def sMsetList = MGroup.parseSlots('mset', data5, slotNumber)
        def s = MGroup.parseSlots('mxxx', data5, slotNumber)

        then:
        sMgetList.size() == 4
        sMsetList.size() == 2
        s.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'view-persist-key-count'
        data4[2] = '0'.bytes
        def sManageList = MGroup.parseSlots('manage', data4, slotNumber)

        then:
        sManageList.size() == 1

        when:
        data4[1] = 'view-slot-bucket-keys'
        data4[2] = '0'.bytes
        sManageList = MGroup.parseSlots('manage', data4, slotNumber)

        then:
        sManageList.size() == 1

        when:
        data4[2] = 'a'.bytes
        sManageList = MGroup.parseSlots('manage', data4, slotNumber)

        then:
        sManageList.size() == 0

        when:
        data4[1] = 'output-dict-bytes'
        data4[2] = '100'.bytes
        data4[3] = 'key:000000000001'.bytes
        sManageList = MGroup.parseSlots('manage', data4, slotNumber)

        then:
        sManageList.size() == 1

        when:
        def data1 = new byte[1][]
        def sList = MGroup.parseSlots('mget', data1, slotNumber)

        then:
        sList.size() == 0

        when:
        sList = MGroup.parseSlots('mset', data1, slotNumber)

        then:
        sList.size() == 0

        when:
        sList = MGroup.parseSlots('manage', data1, slotNumber)

        then:
        sList.size() == 0

        when:
        sList = MGroup.parseSlots('mset', data4, slotNumber)

        then:
        sList.size() == 0

        when:
        data5[1] = 'view-persist-key-count'

        sList = MGroup.parseSlots('manage', data5, slotNumber)

        then:
        sList.size() == 0

        when:
        data5[1] = 'view-slot-bucket-keys'
        sList = MGroup.parseSlots('manage', data5, slotNumber)

        then:
        sList.size() == 0

        when:
        data5[1] = 'output-dict-bytes'
        sList = MGroup.parseSlots('manage', data5, slotNumber)

        then:
        sList.size() == 0

        when:
        data5[1] = 'xxx'
        sList = MGroup.parseSlots('manage', data5, slotNumber)

        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def mGroup = new MGroup('mget', data1, null)
        mGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = mGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        mGroup.cmd = 'mset'
        reply = mGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        mGroup.cmd = 'manage'
        reply = mGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        mGroup.cmd = 'zzz'
        reply = mGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test mget'() {
        given:
        final byte slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('mget', data3, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        mGroup.slotWithKeyHashListParsed = MGroup.parseSlots('mget', data3, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.mget()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = mGroup.mget()

        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == cv.compressedData
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        var eventloop = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        Thread.start {
            eventloop.run()
        }

        LocalPersist.instance.addOneSlotForTest(slot, eventloop)

        mGroup.isCrossRequestWorker = true
        reply = mGroup.mget()

        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test mset'() {
        given:
        final byte slot = 0

        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '1'.bytes
        data5[3] = 'b'.bytes
        data5[4] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('mset', data5, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        mGroup.slotWithKeyHashListParsed = MGroup.parseSlots('mset', data5, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.mset()

        then:
        reply == OKReply.INSTANCE

        when:
        def valA = mGroup.get('a'.bytes, mGroup.slotWithKeyHashListParsed[0])
        def valB = mGroup.get('b'.bytes, mGroup.slotWithKeyHashListParsed[1])

        then:
        valA == '1'.bytes
        valB == '2'.bytes

        when:
        data5[2] = '11'.bytes
        data5[4] = '22'.bytes

        var eventloop = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        Thread.start {
            eventloop.run()
        }

        LocalPersist.instance.addOneSlotForTest(slot, eventloop)

        mGroup.isCrossRequestWorker = true
        reply = mGroup.mset()

        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        valA = mGroup.get('a'.bytes, mGroup.slotWithKeyHashListParsed[0])
        valB = mGroup.get('b'.bytes, mGroup.slotWithKeyHashListParsed[1])

        then:
        valA == '11'.bytes
        valB == '22'.bytes

        cleanup:
        eventloop.breakEventloop()
    }
}
