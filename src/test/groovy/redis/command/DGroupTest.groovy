package redis.command

import io.activej.eventloop.Eventloop
import redis.BaseCommand
import redis.CompressedValue
import redis.mock.InMemoryGetSet
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.persist.Mock
import redis.reply.*
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class DGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[2][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes

        when:
        def sDecr = DGroup.parseSlot('decr', data, slotNumber)
        def sDecrBy = DGroup.parseSlot('decrby', data, slotNumber)
        def sListDel = DGroup.parseSlots('del', data, slotNumber)
        def sListDecr = DGroup.parseSlots('decr', data, slotNumber)
        def s = DGroup.parseSlot('dxxx', data, slotNumber)

        then:
        sDecr != null
        sDecrBy != null
        sListDel.size() == 1
        sListDecr.size() == 1
        s == null

        when:
        data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes

        sListDel = DGroup.parseSlots('del', data, slotNumber)

        then:
        sListDel.size() == 2

        when:
        data = new byte[1][]

        sListDel = DGroup.parseSlots('del', data, slotNumber)
        sDecrBy = DGroup.parseSlot('decrby', data, slotNumber)

        then:
        sListDel.size() == 0
        sDecrBy == null
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('del', data, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('del', data, dGroup.slotNumber)

        when:
        dGroup.handle()
        dGroup.cmd = 'dbsize'
        dGroup.handle()
        dGroup.data = data
        dGroup.cmd = 'decr'
        dGroup.handle()
        dGroup.cmd = 'decrby'
        dGroup.handle()

        then:
        1 == 1

        when:
        dGroup.data = new byte[1][]
        dGroup.cmd = 'decr'
        def r = dGroup.handle()

        then:
        r == ErrorReply.FORMAT

        when:
        data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes
        dGroup.data = data
        dGroup.cmd = 'decrby'
        r = dGroup.handle()

        then:
        r == ErrorReply.NOT_INTEGER

        when:
        // decrby
        data[1] = 'n'.bytes
        data[2] = '1'.bytes
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('decrby', data, dGroup.slotNumber)

        dGroup.setNumber('n'.bytes, 0, dGroup.slotWithKeyHashListParsed[0])
        r = dGroup.handle()

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == -1

        when:
        dGroup.cmd = 'zzz'
        def reply = dGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test del'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('del', data, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        when:
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('del', data, dGroup.slotNumber)
        def r = dGroup.del()

        then:
        r == ErrorReply.KEY_TOO_LONG

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)

        data[1] = 'a'.bytes
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('del', data, dGroup.slotNumber)
        r = dGroup.handle()

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == 1

        when:
        r = dGroup.handle()

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == 0

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

        dGroup.isCrossRequestWorker = true

        data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes
        dGroup.data = data
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('del', data, dGroup.slotNumber)

        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, 'b', 0, cv)
        r = dGroup.handle()

        then:
        r instanceof AsyncReply
        ((AsyncReply) r).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 1
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test dbsize'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[1][]

        def dGroup = new DGroup('dbsize', data, null)
        dGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        and:
        LocalPersistTest.prepareLocalPersist()

        and:
        var eventloop = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        Thread.start {
            eventloop.run()
        }

        LocalPersist.instance.oneSlot(slot).netWorkerEventloop = eventloop

        when:
        def r = dGroup.dbsize()

        then:
        r instanceof AsyncReply
        ((AsyncReply) r).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 0
        }.result

        cleanup:
        eventloop.breakEventloop()
        LocalPersist.instance.cleanUp()
    }

    def 'test decr by'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[2][]
        data[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def dGroup = new DGroup('decrby', data, null)
        dGroup.byPassGetSet = inMemoryGetSet
        dGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        when:
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('decrby', data, dGroup.slotNumber)

        def cv = new CompressedValue()
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        // 0
        cv.compressedData = new byte[1]
        cv.compressedLength = 1

        inMemoryGetSet.put(slot, 'a', 0, cv)
        def r = dGroup.decrBy(1, 0)

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == -1

        when:
        data[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('decrby', data, dGroup.slotNumber)

        r = dGroup.decrBy(1, 0)

        then:
        r == ErrorReply.KEY_TOO_LONG

        when:
        data[1] = 'a'.bytes
        dGroup.slotWithKeyHashListParsed = DGroup.parseSlots('decrby', data, dGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')

        r = dGroup.decrBy(1, 0)

        then:
        r == ErrorReply.NOT_INTEGER

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH_COMPRESSED
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = dGroup.decrBy(1, 0)

        then:
        r == ErrorReply.NOT_INTEGER

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = dGroup.decrBy(1, 0)

        then:
        r == ErrorReply.NOT_INTEGER

        when:
        cv.compressedLength = 4
        cv.compressedData = '1234'.bytes

        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = dGroup.decrBy(1, 0)

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == 1233

        when:
        // float
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_DOUBLE
        cv.compressedLength = 8
        def doubleBytes = new byte[8]
        ByteBuffer.wrap(doubleBytes).putDouble(1.1)
        cv.compressedData = doubleBytes

        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = dGroup.decrBy(0, 1)

        then:
        r instanceof BulkReply
        ((BulkReply) r).raw == '0.10'.bytes

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cv.compressedLength = 3
        cv.compressedData = '1.1'.bytes

        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = dGroup.decrBy(0, 1)

        then:
        r instanceof BulkReply
        ((BulkReply) r).raw == '0.10'.bytes
    }
}
