package redis.persist

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import net.openhft.affinity.AffinityStrategies
import net.openhft.affinity.AffinityThreadFactory
import redis.ConfForSlot
import redis.Utils
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class FdReadWriteTest extends Specification {
    def "test read write"() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
        def oneFile = new File('/tmp/test-fd-read-write')
        if (oneFile.exists()) {
            oneFile.delete()
        }
        def oneFile2 = new File('/tmp/test-fd-read-write2')
        if (oneFile2.exists()) {
            oneFile2.delete()
        }

        and:
        def threadFactory = new AffinityThreadFactory('fd-read-write-group-1', AffinityStrategies.SAME_CORE)

        def fdReadWrite = new FdReadWrite('test', libC, oneFile)
        fdReadWrite.initByteBuffers(true)
        fdReadWrite.initEventloop(threadFactory)

        def fdReadWrite2 = new FdReadWrite('test2', libC, oneFile2)
        fdReadWrite2.initByteBuffers(true)
        fdReadWrite2.initEventloop(threadFactory)

        def segmentLength = ConfForSlot.global.confChunk.segmentLength

        int loop = 10

        when:
        CompletableFuture<Integer>[] futures = new CompletableFuture<Integer>[loop * 2]
        loop.times { i ->
            var bytes = new byte[segmentLength]
            Arrays.fill(bytes, (byte) i)
            var f = fdReadWrite.writeSegment(i, bytes, false)
            var f2 = fdReadWrite2.writeSegment(i, bytes, false)
            futures[i] = f
            futures[i + loop] = f2
        }

        CompletableFuture.allOf(futures).join()

        then:
        futures.every { it.get() == segmentLength }

        cleanup:
        fdReadWrite.cleanUp()
        fdReadWrite2.cleanUp()

        def stats = fdReadWrite.stats()
        println Utils.padStats(stats, 60)

        def sw = new StringWriter()
        TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples());
        println sw.toString()
    }
}
