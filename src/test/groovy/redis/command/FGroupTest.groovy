package redis.command

import io.activej.eventloop.Eventloop
import redis.BaseCommand
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.reply.AsyncReply
import redis.reply.NilReply
import redis.reply.OKReply
import spock.lang.Specification

import java.time.Duration

class FGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sFlushDbList = FGroup.parseSlots('flushdb', data2, slotNumber)
        def sFlushAll = FGroup.parseSlot('flushall', data2, slotNumber)
        def s = FGroup.parseSlot('fxxx', data2, slotNumber)

        then:
        sFlushDbList.size() == 1
        sFlushDbList[0] == null
        sFlushAll == null
        s == null
    }

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def fGroup = new FGroup('flushdb', data2, null)
        fGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = fGroup.handle()

        then:
        reply == OKReply.INSTANCE

        when:
        fGroup.cmd = 'flushall'
        reply = fGroup.handle()

        then:
        reply == OKReply.INSTANCE

        when:
        fGroup.cmd = 'zzz'
        reply = fGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test flushdb'() {
        given:
        final byte slot = 0

        def data1 = new byte[1][]

        def fGroup = new FGroup('flushdb', data1, null)
        fGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

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

        LocalPersist.instance.addOneSlotForTest(slot, eventloop)

        when:
        def r = fGroup.flushdb()

        then:
        r instanceof AsyncReply
        ((AsyncReply) r).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
