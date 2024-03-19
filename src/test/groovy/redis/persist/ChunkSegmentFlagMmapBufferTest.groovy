package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class ChunkSegmentFlagMmapBufferTest extends Specification {
    LibC libC

    void setup() {
        System.setProperty("jnr.ffi.asm.enabled", "false")
        libC = LibraryLoader.create(LibC.class).load("c")
    }

    def "set and get"() {
        given:
        def file = new File('/tmp/test-segment-flag-mmap-buffer')
        if (file.exists()) {
            file.delete()
        }

        var x = new ChunkSegmentFlagMmapBuffer(file, (byte) 3)
        x.libC = libC
        x.init()

        and:
        HashSet<String> set = []
        HashSet<String> set2 = []

        def snowFlake = new SnowFlake(1, 1)

        100.times {
            def random = new Random()
            var randSegmentIndex = random.nextInt(ConfForSlot.global.confChunk.maxSegmentNumber())
            def segmentSeq = snowFlake.nextId()
            x.setSegmentMergeFlag((byte) 0, (byte) 0, randSegmentIndex, (byte) 1, (byte) 1, segmentSeq)

            set << "$randSegmentIndex-$segmentSeq"
        }

        and:
        int count = 0
        x.iterate { workerId, byte batchIndex, segmentIndex, flag, flagWorkerId, segmentSeq ->
            if (flag == 1) {
                println "workerId: $workerId, batchIndex: $batchIndex, segmentIndex: $segmentIndex, flag: $flag, flagWorkerId: $flagWorkerId, segmentSeq: $segmentSeq"
                count++

                set2 << "$segmentIndex-$segmentSeq"
            }
        }
        println "count: $count"

        expect:
        assert set == set2

        cleanup:
        x.cleanUp()
    }
}
