package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class StatKeyCountInBucketsTest extends Specification {
    def 'test for repl'() {
        given:
        def one = new StatKeyCountInBuckets((byte) 0, 4096, slotDir)
        def two = new StatKeyCountInBuckets((byte) 0, 4096, slotDir)

        when:
        def allInMemoryCachedBytes = one.getInMemoryCachedBytes()
        then:
        allInMemoryCachedBytes.length == one.allCapacity

        when:
        def bytes0 = new byte[one.allCapacity]
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == one.allCapacity

        when:
        ConfForSlot.global.pureMemory = true
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == one.allCapacity

        when:
        boolean exception = false
        def bytes0WrongSize = new byte[one.allCapacity - 1]
        try {
            one.overwriteInMemoryCachedBytes(bytes0WrongSize)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

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

        def one = new StatKeyCountInBuckets((byte) 0, 4096, slotDir)

        when:
        one.setKeyCountForBucketIndex(10, (short) 10)
        one.setKeyCountForBucketIndex(20, (short) 20)
        then:
        one.getKeyCountForBucketIndex(10) == 10
        one.getKeyCountForBucketIndex(20) == 20
        one.getKeyCount() == 30

        when:
        ConfForSlot.global.pureMemory = true
        def one2 = new StatKeyCountInBuckets((byte) 0, 4096, slotDir)
        one2.setKeyCountForBucketIndex(10, (short) 10)
        one2.setKeyCountForBucketIndex(20, (short) 20)
        then:
        one2.getKeyCountForBucketIndex(10) == 10
        one2.getKeyCountForBucketIndex(20) == 20
        one2.getKeyCount() == 30

        cleanup:
        ConfForSlot.global.pureMemory = false
        one.clear()
        one.cleanUp()
        ConfForSlot.global.pureMemory = true
        one2.clear()
        one2.cleanUp()
        ConfForSlot.global.pureMemory = false
        slotDir.deleteDir()
    }
}
