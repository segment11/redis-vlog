package redis.repl

import spock.lang.Specification

class MetaKeyBucketSeqTest extends Specification {
    private static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    def setup() {
        slotDir.mkdirs()
    }

    def "read write seq"() {
        given:
        def seq = new MetaKeyBucketSeq((byte) 0, 4096, slotDir)
        when:
        seq.writeSeq(0, (byte) 0, 1L)
        then:
        seq.readSeq(0, (byte) 0) == 1L
        cleanup:
        seq.clear()
        seq.cleanUp()
    }

    def "read write batch"() {
        given:
        def seq = new MetaKeyBucketSeq((byte) 0, 4096, slotDir)
        when:
        seq.writeSeqBatch(0, (byte) 0, [1L, 2L])
        def result = seq.readSeqBatch(0, (byte) 0, 2)
        then:
        result[0].seq() == 1L
        result[0].bucketIndex() == 0
        result[1].seq() == 2L
        result[1].bucketIndex() == 1
        cleanup:
        seq.clear()
        seq.cleanUp()
    }
}
