package redis.persist

import io.activej.config.Config
import redis.SnowFlake
import spock.lang.Specification

class LocalPersistTest extends Specification {
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
        SnowFlake[] snowFlakes = new SnowFlake[1]
        snowFlakes[0] = new SnowFlake(1, 1)
        localPersist.initSlots((byte) 1, (short) 1, snowFlakes, Consts.persistDir, Config.create())
        localPersist.debugMode()
        localPersist.fixSlotThreadId((byte) 0, 0L)
        localPersist.persistMergeSegmentsUndone()

        then:
        localPersist.oneSlots().length == 1
        localPersist.oneSlot((byte) 0) != null

        cleanup:
        localPersist.cleanUp()
    }
}
