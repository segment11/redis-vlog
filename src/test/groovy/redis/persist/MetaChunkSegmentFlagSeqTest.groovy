package redis.persist

import redis.ConfForSlot
import spock.lang.Specification

import static redis.persist.Chunk.NO_NEED_MERGE_SEGMENT_INDEX
import static redis.persist.Consts.getSlotDir

class MetaChunkSegmentFlagSeqTest extends Specification {
    def "read write seq"() {
        given:
        ConfForSlot.global.pureMemory = false
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        one.setSegmentMergeFlag(10, (byte) 1, 1L, 0)
        def segmentFlag = one.getSegmentMergeFlag(10)
        then:
        segmentFlag.flag == 1
        segmentFlag.segmentSeq == 1L
        segmentFlag.walGroupIndex == 0

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def "read write seq pure memory"() {
        given:
        ConfForSlot.global.pureMemory = true
        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        one.setSegmentMergeFlag(10, (byte) 1, 1L, 0)
        one.setSegmentMergeFlag(11, (byte) 2, 2L, 11)
        def segmentFlag = one.getSegmentMergeFlag(10)
        def segmentFlagList = one.getSegmentMergeFlagBatch(10, 2)

        then:
        segmentFlag.flag == 1
        segmentFlag.segmentSeq == 1L
        segmentFlag.walGroupIndex == 0

        segmentFlagList.size() == 2
        segmentFlagList[0].flag == 1
        segmentFlagList[0].segmentSeq == 1L
        segmentFlagList[0].walGroupIndex == 0
        segmentFlagList[1].flag == 2
        segmentFlagList[1].segmentSeq == 2L
        segmentFlagList[1].walGroupIndex == 11

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
        one.setSegmentMergeFlagBatch(10, 10, (byte) 1, seqLongList, 0)

        then:
        one.getSegmentSeqListBatchForRepl(10, 10) == seqLongList

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'iterate'() {
        given:
        def confChunk = ConfForSlot.global.confChunk
        def targetConfChunk = ConfForSlot.ConfChunk.c1m
//        def targetConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = targetConfChunk.segmentNumberPerFd
        confChunk.fdPerChunk = targetConfChunk.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        when:
        new File('chunk_segment_flag.txt').withWriter { writer ->
            one.iterateAll { segmentIndex, flag, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flag, $seq, $walGroupIndex")
            }
        }

        new File('chunk_segment_flag_range.txt').withWriter { writer ->
            one.iterateRange(1024, 1024) { segmentIndex, flag, seq, walGroupIndex ->
                writer.writeLine("$segmentIndex, $flag, $seq, $walGroupIndex")
            }
        }

        then:
        1 == 1

        cleanup:
        one.clear()
        one.cleanUp()
    }

    def 'iterate and find'() {
        given:
        def confChunk = ConfForSlot.global.confChunk
        def targetConfChunk = ConfForSlot.ConfChunk.c100m
//        def targetConfChunk = ConfForSlot.ConfChunk.debugMode
        confChunk.segmentNumberPerFd = targetConfChunk.segmentNumberPerFd
        confChunk.fdPerChunk = targetConfChunk.fdPerChunk

        def one = new MetaChunkSegmentFlagSeq((byte) 0, slotDir)

        and:
        final byte slot = 0
        var chunk = new Chunk(slot, Consts.slotDir, null, null, null, null)

        when:

        def r = one.iterateAndFind(1024, 1024 * 10, 1, chunk)

        then:
        r[0] == NO_NEED_MERGE_SEGMENT_INDEX
        r[1] == 0

        cleanup:
        one.clear()
        one.cleanUp()
    }
}
