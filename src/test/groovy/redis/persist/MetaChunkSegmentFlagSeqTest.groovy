package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentFlagSeqTest extends Specification {
    def "read write seq"() {
        given:
        ConfForSlot.global.pureMemory = false
        def seq = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        seq.setSegmentMergeFlag(10, (byte) 1, 1L)
        then:
        seq.getSegmentMergeFlag(10).flag() == 1
        seq.getSegmentMergeFlag(10).segmentSeq() == 1L

        cleanup:
        seq.clear()
        seq.cleanUp()
    }

    def "read write seq pure memory"() {
        given:
        ConfForSlot.global.pureMemory = true
        def seq = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        seq.setSegmentMergeFlag(10, (byte) 1, 1L)
        then:
        seq.getSegmentMergeFlag(10).flag() == 1
        seq.getSegmentMergeFlag(10).segmentSeq() == 1L

        cleanup:
        seq.clear()
        seq.cleanUp()
    }

    def 'read batch for repl'() {
        given:
        def seq = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        List<Long> seqLongList = []
        10.times {
            seqLongList << (it as Long)
        }
        seq.setSegmentMergeFlagBatch(10, 10, (byte) 1, seqLongList)

        then:
        seq.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        cleanup:
        seq.clear()
        seq.cleanUp()
    }
}
