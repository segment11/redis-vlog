package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentFlagSeqTest extends Specification {
    def "read write seq"() {
        given:
        ConfForSlot.global.pureMemory = false
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        one.setSegmentMergeFlag(10, (byte) 1, 1L)
        then:
        one.getSegmentMergeFlag(10).flag() == 1
        one.getSegmentMergeFlag(10).segmentSeq() == 1L

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def "read write seq pure memory"() {
        given:
        ConfForSlot.global.pureMemory = true
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        one.setSegmentMergeFlag(10, (byte) 1, 1L)
        then:
        one.getSegmentMergeFlag(10).flag() == 1
        one.getSegmentMergeFlag(10).segmentSeq() == 1L

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'read batch for repl'() {
        given:
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        List<Long> seqLongList = []
        10.times {
            seqLongList << (it as Long)
        }
        one.setSegmentMergeFlagBatch(10, 10, (byte) 1, seqLongList)

        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'iterate'() {
        given:
        def confChunk = ConfForSlot.global.confChunk
        def debugConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = debugConfChunk.segmentNumberPerFd
        confChunk.fdPerChunk = debugConfChunk.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        one.iterate { segmentIndex, flag, seq ->
            println "segment index: $segmentIndex, flag: $flag, seq: $seq"
        }

        then:
        1 == 1

        cleanup:
        one.clear()
        one.cleanUp()
    }
}
