package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaOneWalGroupSeqTest extends Specification {
    final byte slot = 0

    def 'test set and get'() {
        given:
        ConfForSlot.global.pureMemory = false

        def one = new MetaOneWalGroupSeq(slot, slotDir)
        println one.inMemoryCachedBytes.length

        when:
        one.set(0, (byte) 0, 1L)
        one.set(1, (byte) 0, 1L)
        then:
        one.get(0, (byte) 0) == 1L
        one.get(1, (byte) 0) == 1L

        when:
        one.cleanUp()
        def one2 = new MetaOneWalGroupSeq(slot, slotDir)
        then:
        one2.get(0, (byte) 0) == 1L

        when:
        one2.clear()
        one2.cleanUp()
        ConfForSlot.global.pureMemory = true
        def one3 = new MetaOneWalGroupSeq(slot, slotDir)
        one3.set(0, (byte) 0, 1L)
        then:
        one3.get(0, (byte) 0) == 1L

        cleanup:
        one3.clear()
        one3.cleanUp()
        ConfForSlot.global.pureMemory = false
        slotDir.deleteDir()
    }
}
