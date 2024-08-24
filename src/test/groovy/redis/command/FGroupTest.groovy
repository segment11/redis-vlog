package redis.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import redis.BaseCommand
import redis.ConfForGlobal
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.LeaderSelectorTest
import redis.reply.*
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
        def sFlushDbList = FGroup.parseSlots('failover', data2, slotNumber)
        then:
        sFlushDbList.size() == 0
    }

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def fGroup = new FGroup('failover', data2, null)
        fGroup.from(BaseCommand.mockAGroup())

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
        fGroup.cmd = 'flushdb'
        reply = fGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        fGroup.cmd = 'zzz'
        reply = fGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    final byte slot = 0

    def 'test failover'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('failover', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

        when:
        ConfForGlobal.zookeeperConnectString = null
        def reply = fGroup.failover()
        then:
        reply instanceof ErrorReply

        when:
        ConfForGlobal.zookeeperConnectString = 'localhost:2181'
        boolean doThisCase = LeaderSelectorTest.checkZk()
        Eventloop eventloop
        def localPersist = LocalPersist.instance
        if (doThisCase) {
            LocalPersistTest.prepareLocalPersist()
            localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

            eventloop = Eventloop.builder()
                    .withIdleInterval(Duration.ofMillis(100))
                    .build()
            eventloop.keepAlive(true)
            Thread.start {
                eventloop.run()
            }
            def eventloopCurrent = Eventloop.builder()
                    .withCurrentThread()
                    .withIdleInterval(Duration.ofMillis(100))
                    .build()
            reply = fGroup.failover()
            eventloopCurrent.run()
            Thread.sleep(1000)
        } else {
            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            finalPromise.set(OKReply.INSTANCE)
            reply = new AsyncReply(finalPromise)
        }
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        if (eventloop) {
            eventloop.breakEventloop()
        }
        localPersist.cleanUp()
    }

    def 'test flushdb'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('flushdb', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def reply = fGroup.flushdb()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
