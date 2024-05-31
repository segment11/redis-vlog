package redis.persist

import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaKeyBucketSplitNumberTest extends Specification {
    def "set and get"() {
        given:
        def one = new MetaKeyBucketSplitNumber((byte) 0, slotDir)
        println one.inMemoryCachedBytes

        when:
        one.setForTest(10, (byte) 3)
        one.setForTest(20, (byte) 9)
        one.setForTest(30, (byte) 27)
        then:
        one.get((byte) 10) == 3
        one.get((byte) 20) == 9
        one.get((byte) 30) == 27

        when:
        byte[] splitNumberArray = [3, 9, 27]
        one.setBatch(10, splitNumberArray)
        then:
        one.get((byte) 10) == 3
        one.get((byte) 11) == 9
        one.get((byte) 12) == 27
        one.getBatch(10, 3) == splitNumberArray

        cleanup:
        one.clear()
        one.cleanUp()
    }
}
