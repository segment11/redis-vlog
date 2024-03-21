package redis.persist

import spock.lang.Specification

class MetaChunkSegmentFlagSeqTest extends Specification {
    private static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    def setup() {
        slotDir.mkdirs()
    }

    def "read write seq"() {
        given:
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
