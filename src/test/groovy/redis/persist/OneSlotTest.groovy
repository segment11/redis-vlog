package redis.persist

import io.activej.eventloop.Eventloop
import spock.lang.Specification

import java.time.Duration

class OneSlotTest extends Specification {
    def 'test mock'() {
        given:
        final byte slot = 0

        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)

        when:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        oneSlot.metaChunkSegmentIndex.cleanUp()

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def oneSlot2 = new OneSlot(slot, eventloopCurrent)
        oneSlot2.threadIdProtectedForSafe = Thread.currentThread().threadId()
        def call = oneSlot2.asyncCall(() -> 1)
        def run = oneSlot2.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        call.result
        run != null

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        def oneSlot3 = new OneSlot(slot, eventloop)
        Thread.start {
            eventloop.run()
        }
        call = oneSlot3.asyncCall(() -> 1)
        run = oneSlot3.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        call.whenResult { result -> result == 1 }.result
        run != null

        cleanup:
        eventloop.breakEventloop()
    }
}
