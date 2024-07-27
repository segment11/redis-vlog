package redis.persist

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import redis.SnowFlake
import spock.lang.Specification

import java.time.Duration

class LocalPersistTest extends Specification {
    static void prepareLocalPersist(byte netWorkers = 1, short slotNumber = 1) {
        def localPersist = LocalPersist.instance

        SnowFlake[] snowFlakes = new SnowFlake[1]
        snowFlakes[0] = new SnowFlake(1, 1)
        localPersist.initSlots(netWorkers, slotNumber, snowFlakes, Consts.persistDir, Config.create())
        localPersist.debugMode()
    }

    def 'test all'() {
        given:
        def localPersist = LocalPersist.instance

        expect:
        LocalPersist.PAGE_SIZE == 4096
        LocalPersist.PROTECTION == 7
        LocalPersist.DEFAULT_SLOT_NUMBER == 4
        LocalPersist.MAX_SLOT_NUMBER == 128
        LocalPersist.O_DIRECT == 040000
        localPersist.oneSlots() == null

        when:
        prepareLocalPersist()
        localPersist.fixSlotThreadId((byte) 0, Thread.currentThread().threadId())
        localPersist.persistMergeSegmentsUndone()
        then:
        localPersist.oneSlots().length == 1
        localPersist.oneSlot((byte) 0) != null
        localPersist.currentThreadFirstOneSlot() == localPersist.oneSlots()[0]

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test multi slot'() {
        given:
        def localPersist = LocalPersist.instance

        when:
        prepareLocalPersist((byte) 1, (short) 2)
        localPersist.fixSlotThreadId((byte) 1, Thread.currentThread().threadId())
        then:
        localPersist.oneSlots().length == 2
        localPersist.currentThreadFirstOneSlot() == localPersist.oneSlots()[1]

        when:
        boolean exception = false
        localPersist.fixSlotThreadId((byte) 1, -1L)
        try {
            localPersist.currentThreadFirstOneSlot()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        localPersist.fixSlotThreadId((byte) 0, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((byte) 1, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test mock one slot'() {
        given:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        def localPersist = LocalPersist.instance
        localPersist.addOneSlotForTest((byte) 0, eventloop)
        localPersist.addOneSlotForTest((byte) 0, null)

        expect:
        localPersist.oneSlots().length == 1
        localPersist.firstSlot() == (byte) 0

        when:
        localPersist.socketInspector = null
        then:
        localPersist.socketInspector == null

        cleanup:
        eventloop.breakEventloop()
    }
}
