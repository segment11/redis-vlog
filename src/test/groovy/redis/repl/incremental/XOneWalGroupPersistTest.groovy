package redis.repl.incremental

import redis.ConfForSlot
import redis.persist.Chunk
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.BinlogContent
import redis.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XOneWalGroupPersistTest extends Specification {
    def 'test encode and decode'() {
        given:
        def x = new XOneWalGroupPersist(true, 0)

        expect:
        x.type() == BinlogContent.Type.one_wal_group_persist

        when:
        x.beginBucketIndex = 0
        x.keyCountForStatsTmp = [1, 2, 3]
        def sharedBytesList = new byte[3][]
        sharedBytesList[0] = new byte[ConfForSlot.global.confWal.oneChargeBucketNumber * 4096]
        x.sharedBytesListBySplitIndex = sharedBytesList
        x.oneWalGroupSeqArrayBySplitIndex = [0L, 1L, 2L]
        byte[] splitNumberAfterPut = [3, 3, 3]
        x.splitNumberAfterPut = splitNumberAfterPut
        x.putUpdatedChunkSegmentFlagWithSeq(0, Chunk.Flag.new_write, 0L)
        x.putUpdatedChunkSegmentFlagWithSeq(1, Chunk.Flag.new_write, 1L)
        x.putUpdatedChunkSegmentBytes(0, new byte[4096])

        def encoded = x.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def x2 = XOneWalGroupPersist.decodeFrom(buffer)
        then:
        x2.encodedLength() == encoded.length

        when:
        final byte slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def replPair = ReplPairTest.mockAsSlave()
        x.apply(slot, replPair)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
