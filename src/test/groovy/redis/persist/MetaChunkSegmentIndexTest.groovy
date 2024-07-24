package redis.persist

import redis.ConfForSlot
import redis.repl.Binlog
import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentIndexTest extends Specification {
    final byte slot = 0

    def 'test for repl'() {
        given:
        def one = new MetaChunkSegmentIndex(slot, slotDir)
        def one2 = new MetaChunkSegmentIndex(slot, slotDir)

        when:
        one2.cleanUp()
        ConfForSlot.global.pureMemory = true
        def two = new MetaChunkSegmentIndex(slot, slotDir)
        then:
        two.get() == one.get()

        cleanup:
        one.clear()
        one.cleanUp()
        two.cleanUp()
        ConfForSlot.global.pureMemory = false
        slotDir.deleteDir()
    }

    def 'test set and get'() {
        given:
        ConfForSlot.global.pureMemory = false
        def one = new MetaChunkSegmentIndex(slot, slotDir)

        when:
        one.set(10)
        then:
        one.get() == 10

        when:
        one.set(20)
        then:
        one.get() == 20

        when:
        one.setMasterBinlogFileIndexAndOffset(10L, 1, 0L)
        then:
        one.masterBinlogFileIndexAndOffset == new Binlog.FileIndexAndOffset(1, 0L)

        when:
        one.setAll(30, 1L, 2, 0)
        then:
        one.get() == 30
        one.masterUuid == 1L
        one.masterBinlogFileIndexAndOffset == new Binlog.FileIndexAndOffset(2, 0)

        when:
        one.clear()
        then:
        one.get() == 0

        when:
        ConfForSlot.global.pureMemory = true
        def one2 = new MetaChunkSegmentIndex(slot, slotDir)
        one2.set(10)
        then:
        one2.get() == 10

        cleanup:
        ConfForSlot.global.pureMemory = false
        one.cleanUp()
        ConfForSlot.global.pureMemory = true
        one2.cleanUp()
        ConfForSlot.global.pureMemory = false
        slotDir.deleteDir()
    }
}
