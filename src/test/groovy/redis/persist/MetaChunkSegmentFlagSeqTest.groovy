package redis.persist

import redis.ConfForSlot
import spock.lang.Specification
import static Consts.*

class MetaChunkSegmentFlagSeqTest extends Specification {
    def "read write seq"() {
        given:
        ConfForSlot.global.pureMemory = false

        def seq = new MetaChunkSegmentFlagSeq((byte) 0, (byte) 3, slotDir)
        when:
        seq.setSegmentMergeFlag((byte) 1, (byte) 1, 10, (byte) 1, (byte) 1, 1L)
        then:
        seq.getSegmentMergeFlag((byte) 1, (byte) 1, 10).flag() == 1
        cleanup:
        seq.clear()
        seq.cleanUp()
    }


    def "read write seq pure memory"() {
        given:
        ConfForSlot.global.pureMemory = true

        def seq = new MetaChunkSegmentFlagSeq((byte) 0, (byte) 3, slotDir)
        when:
        seq.setSegmentMergeFlag((byte) 1, (byte) 1, 10, (byte) 1, (byte) 1, 1L)
        then:
        seq.getSegmentMergeFlag((byte) 1, (byte) 1, 10).flag() == 1
        cleanup:
        seq.clear()
        seq.cleanUp()
    }
}
