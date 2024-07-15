package redis.command

import io.activej.eventloop.Eventloop
import redis.BaseCommand
import redis.CompressedValue
import redis.mock.InMemoryGetSet
import redis.persist.LocalPersist
import redis.persist.Mock
import redis.reply.AsyncReply
import redis.reply.ErrorReply
import redis.reply.IntegerReply
import redis.reply.NilReply
import spock.lang.Specification

import java.time.Duration

class EGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sExistsList = EGroup.parseSlots('exists', data2, slotNumber)
        def sExpireList = EGroup.parseSlots('expire', data2, slotNumber)
        def sExpireAtList = EGroup.parseSlots('expireat', data2, slotNumber)
        def sExpireTimeList = EGroup.parseSlots('expiretime', data2, slotNumber)
        def sList = EGroup.parseSlots('exxx', data2, slotNumber)

        then:
        sExistsList.size() == 1
        sExpireList.size() == 1
        sExpireAtList.size() == 1
        sExpireTimeList.size() == 1
        sList.size() == 0

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        sExistsList = EGroup.parseSlots('exists', data3, slotNumber)

        then:
        sExistsList.size() == 2

        when:
        def data1 = new byte[1][]

        sExistsList = EGroup.parseSlots('exists', data1, slotNumber)
        sExpireList = EGroup.parseSlots('expire', data1, slotNumber)

        then:
        sExistsList.size() == 0
        sExpireList.size() == 0
    }

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup('exists', data2, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        eGroup.slotWithKeyHashListParsed = EGroup.parseSlots('exists', data2, eGroup.slotNumber)

        when:
        eGroup.handle()
        eGroup.cmd = 'expire'
        eGroup.handle()
        eGroup.data = data2
        eGroup.cmd = 'expireat'
        eGroup.handle()
        eGroup.cmd = 'expiretime'
        eGroup.handle()
        eGroup.cmd = 'echo'
        eGroup.handle()

        then:
        1 == 1

        when:
        def data1 = new byte[1][]

        eGroup.data = data1
        eGroup.cmd = 'echo'
        def r = eGroup.handle()

        then:
        r == ErrorReply.FORMAT

        when:
        eGroup.cmd = 'zzz'
        def reply = eGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test exists'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]

        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup('exists', data2, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        when:
        def data1 = new byte[1][]
        eGroup.data = data1
        eGroup.slotWithKeyHashListParsed = DGroup.parseSlots('exists', data1, eGroup.slotNumber)
        def r = eGroup.exists()

        then:
        r == ErrorReply.FORMAT

        when:
        eGroup.data = data2
        eGroup.slotWithKeyHashListParsed = EGroup.parseSlots('exists', data2, eGroup.slotNumber)
        r = eGroup.exists()

        then:
        r == ErrorReply.KEY_TOO_LONG

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)

        data2[1] = 'a'.bytes
        eGroup.slotWithKeyHashListParsed = EGroup.parseSlots('exists', data2, eGroup.slotNumber)
        r = eGroup.exists()

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == 1

        when:
        inMemoryGetSet.remove(slot, 'a')
        r = eGroup.exists()

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == 0

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        Thread.start {
            eventloop.run()
        }

        LocalPersist.instance.addOneSlotForTest(slot, eventloop)

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        eGroup.isCrossRequestWorker = true
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        eGroup.data = data3
        eGroup.slotWithKeyHashListParsed = EGroup.parseSlots('exists', data3, eGroup.slotNumber)

        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, 'b', 0, cv)

        r = eGroup.exists()
        eventloopCurrent.run()

        then:
        r instanceof AsyncReply
        ((AsyncReply) r).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 1
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test expire'() {
        given:
        final byte slot = 0

        def data1 = new byte[1][]

        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup('expire', data1, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        when:
        def r = eGroup.expire(true, true)

        then:
        r == ErrorReply.FORMAT

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes

        eGroup.data = data3
        r = eGroup.expire(true, true)

        then:
        // not exists
        r == IntegerReply.REPLY_0

        when:
        // not valid integer
        data3[2] = 'a'.bytes
        r = eGroup.expire(true, true)

        then:
        r == ErrorReply.NOT_INTEGER

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)

        data3[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data3[2] = (1000 * 60).toString().bytes
        r = eGroup.expire(false, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data3[2] = ((System.currentTimeMillis() / 1000).intValue() + 60).toString().bytes
        r = eGroup.expire(true, false)

        then:
        r == IntegerReply.REPLY_1

        when:
        data3[2] = 60.toString().bytes
        r = eGroup.expire(false, false)

        then:
        r == IntegerReply.REPLY_1

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        data4[3] = 'nx'.bytes

        eGroup.data = data4
        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_0

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        data4[3] = 'xx'.bytes

        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_0

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        data4[3] = 'xx'.bytes

        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 + 1000).toString().bytes
        data4[3] = 'gt'.bytes

        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 - 1000).toString().bytes
        data4[3] = 'gt'.bytes

        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_0

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 - 1000).toString().bytes
        data4[3] = 'lt'.bytes

        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 + 1000).toString().bytes
        data4[3] = 'lt'.bytes

        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_0

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expire(true, true)

        then:
        r == IntegerReply.REPLY_1
    }

    def 'test expiretime'() {
        given:
        final byte slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup('decrby', data2, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        when:
        def data1 = new byte[1][]
        eGroup.data = data1
        eGroup.slotWithKeyHashListParsed = DGroup.parseSlots('expiretime', data1, eGroup.slotNumber)
        def r = eGroup.expiretime(true)

        then:
        r == ErrorReply.FORMAT

        when:
        eGroup.data = data2
        eGroup.slotWithKeyHashListParsed = EGroup.parseSlots('expiretime', data2, eGroup.slotNumber)

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expiretime(true)

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == -1

        when:
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)

        r = eGroup.expiretime(true)

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == cv.expireAt

        when:
        r = eGroup.expiretime(false)

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == (cv.expireAt / 1000).intValue()

        when:
        inMemoryGetSet.remove(slot, 'a')

        r = eGroup.expiretime(true)

        then:
        r instanceof IntegerReply
        ((IntegerReply) r).integer == -2
    }
}
