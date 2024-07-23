package redis.repl

import redis.ConfForSlot
import redis.persist.*
import redis.repl.incremental.XWalV
import spock.lang.Specification

import java.nio.ByteBuffer

class BinlogTest extends Specification {
    def 'test append'() {
        given:
        final byte slot = 0
        ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount = 2

        println new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        def fileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 1)

        expect:
        fileIndexAndOffset == fileIndexAndOffset
        fileIndexAndOffset != null
        fileIndexAndOffset != 1
        fileIndexAndOffset == new Binlog.FileIndexAndOffset(1, 1)
        fileIndexAndOffset != new Binlog.FileIndexAndOffset(1, 2)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 1) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0)

        when:
        var dynConfig = new DynConfig(slot, DynConfigTest.tmpFile)
        var dynConfig2 = new DynConfig(slot, DynConfigTest.tmpFile2)
        dynConfig.binlogOn = true
        dynConfig2.binlogOn = false
        def binlog = new Binlog(slot, Consts.slotDir, dynConfig)
        println binlog.currentFileIndexAndOffset()

        final File slotDir2 = new File('/tmp/redis-vlog/test-persist/test-slot2')
        if (!slotDir2.exists()) {
            slotDir2.mkdir()
            def binlogDir2 = new File(slotDir2, 'binlog')
            if (!binlogDir2.exists()) {
                binlogDir2.mkdir()
            }
            new File(binlogDir2, 'test.txt').text = 'test'
        }
        def binlog2 = new Binlog(slot, slotDir2, dynConfig2)

        and:
        def vList = Mock.prepareValueList(11)
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
            binlog2.append(new XWalV(v))
        }

        then:
        binlog.prevRaf(-1) == null
        binlog.readPrevRafOneSegment(-1, 0) == null
        binlog.readPrevRafOneSegment(1, 0) == null

        when:
        def oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        def oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        binlog.currentFileOffset = oneFileMaxLength - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        then:
        binlog.currentFileIndex == 1
        binlog.prevRaf(0) != null
        binlog.readPrevRafOneSegment(0, 0).length == oneSegmentLength

        when:
        binlog.currentFileOffset = oneSegmentLength - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        binlog.currentFileOffset = oneSegmentLength * 2 - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        binlog.currentFileOffset = oneSegmentLength * 3 - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        then:
        binlog.readCurrentRafOneSegment(0).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(oneSegmentLength).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(oneSegmentLength * 2).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(oneSegmentLength * 3).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(binlog.currentFileOffset) == null

        when:
        def lastAppendFileOffset = binlog.currentFileOffset
        binlog.currentFileOffset = oneSegmentLength * 4
        then:
        binlog.readCurrentRafOneSegment(oneSegmentLength * 3).length > 0

        when:
        // for cache
        def bytes = binlog.readPrevRafOneSegment(binlog.currentFileIndex, oneSegmentLength)
        then:
        bytes != null

        when:
        bytes = binlog.readPrevRafOneSegment(binlog.currentFileIndex, oneSegmentLength * 10)
        then:
        bytes == null

        when:
        boolean exception = false
        try {
            binlog.readPrevRafOneSegment(0, 1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            binlog.readCurrentRafOneSegment(1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        when:
        // current file index == 1
        def oldCurrentFileIndex = binlog.currentFileIndex
        binlog.close()
        // load again
        binlog = new Binlog(slot, Consts.slotDir, dynConfig)
        then:
        binlog.currentFileIndex == oldCurrentFileIndex
        binlog.currentFileOffset == lastAppendFileOffset
        binlog.prevRaf(0) != null

        cleanup:
        binlog.clear()
        binlog.close()
        Consts.slotDir.deleteDir()
        slotDir2.deleteDir()
    }

    def 'test apply'() {
        given:
        final byte slot = 0

        def oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        def oneSegmentBytes = new byte[oneSegmentLength]
        def buffer = ByteBuffer.wrap(oneSegmentBytes)

        and:
        def vList = Mock.prepareValueList(10)
        vList.each { v ->
            def xWalV = new XWalV(v)
            def encoded = xWalV.encodeWithType()
            buffer.put(encoded)
        }

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId((byte) 0, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        Binlog.decodeAndApply(slot, oneSegmentBytes)
        then:
        oneSlot.getWalByBucketIndex(0).keyCount == 10

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
