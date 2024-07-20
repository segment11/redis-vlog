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

        cleanup:
        localPersist.cleanUp()
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

        expect:
        localPersist.oneSlots().length == 1

        cleanup:
        eventloop.breakEventloop()
    }
}
