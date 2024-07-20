package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentIndexTest extends Specification {
    def 'test for repl'() {
        given:
        def one = new MetaChunkSegmentIndex((byte) 0, slotDir)

        when:
        def allInMemoryCachedBytes = one.getInMemoryCachedBytes()
        then:
        allInMemoryCachedBytes.length == 4

        when:
        ConfForSlot.global.pureMemory = false
        def bytes0 = new byte[4]
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == 4

        when:
        ConfForSlot.global.pureMemory = true
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == 4

        when:
        boolean exception = false
        def bytes0WrongSize = new byte[3]
        try {
            one.overwriteInMemoryCachedBytes(bytes0WrongSize)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def two = new MetaChunkSegmentIndex((byte) 0, slotDir)
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
        def one = new MetaChunkSegmentIndex((byte) 0, slotDir)

        when:
        one.set(10)
        then:
        one.get() == 10

        when:
        one.set(20)
        then:
        one.get() == 20

        when:
        one.clear()
        then:
        one.get() == 0

        when:
        ConfForSlot.global.pureMemory = true
        def one2 = new MetaChunkSegmentIndex((byte) 0, slotDir)
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
