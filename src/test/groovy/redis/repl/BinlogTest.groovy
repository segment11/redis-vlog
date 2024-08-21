package redis.repl

import redis.ConfForSlot
import redis.persist.*
import redis.repl.incremental.XWalV
import spock.lang.Specification

import java.nio.ByteBuffer

class BinlogTest extends Specification {
    final byte slot = 0

    def 'test append'() {
        given:
        ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount = 2

        println Binlog.oneFileMaxSegmentCount()

        def bytesWithFileIndexAndOffset = new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        println bytesWithFileIndexAndOffset
        def fileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 1)

        expect:
        !fileIndexAndOffset.equals(null)
        fileIndexAndOffset != bytesWithFileIndexAndOffset
        fileIndexAndOffset == new Binlog.FileIndexAndOffset(1, 1)
        fileIndexAndOffset != new Binlog.FileIndexAndOffset(1, 0)
        fileIndexAndOffset != new Binlog.FileIndexAndOffset(0, 1)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 1) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0)
        Binlog.marginFileOffset(100) == 0
        Binlog.marginFileOffset(1024 * 1024 + 100) == 1024 * 1024

        when:
        def dynConfig = new DynConfig(slot, DynConfigTest.tmpFile)
        def dynConfig2 = new DynConfig(slot, DynConfigTest.tmpFile2)
        dynConfig.binlogOn = true
        dynConfig2.binlogOn = false
        def binlog = new Binlog(slot, Consts.slotDir, dynConfig)
        println binlog.currentFileIndexAndOffset()
        println binlog.earliestFileIndexAndOffset()
        println 'in memory size estimate: ' + binlog.estimate()

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

        when:
        boolean exception = false
        try {
            binlog.readPrevRafOneSegment(1, 0) == null
        } catch (IOException e) {
            println e.message
            exception = true
        }
        then:
        exception

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
        binlog.readPrevRafOneSegment(1, 0).length < oneSegmentLength

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
        exception = false
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

        when:
        ConfForSlot.global.confRepl.binlogFileKeepMaxCount = 1
        binlog.currentFileOffset = oneFileMaxLength - 1
        // trigger remove old file
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        then:
        binlog.currentFileIndex == 2

        when:
        exception = false
        def testBinlogContent = new BinlogContent() {
            @Override
            BinlogContent.Type type() {
                return null
            }

            @Override
            int encodedLength() {
                return 0
            }

            @Override
            byte[] encodeWithType() {
                new byte[oneSegmentLength]
            }

            @Override
            void apply(byte slot, ReplPair replPair) {

            }
        }
        try {
            binlog.append(testBinlogContent)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        // write from master
        when:
        def oneSegmentBytes = new byte[oneSegmentLength]
        // write to current file index
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 2, 0)
        then:
        binlog.currentFileOffset == oneSegmentLength

        when:
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 2, oneFileMaxLength - oneSegmentLength)
        then:
        // create next file
        binlog.currentFileIndex == 3
        binlog.currentFileOffset == 0

        when:
        // write perv again
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 2, 0)
        then:
        // not change
        binlog.currentFileIndex == 3
        binlog.currentFileOffset == 0

        when:
        // write to next file not created yet
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 4, 0)
        then:
        // change to bigger file index
        binlog.currentFileIndex == 4
        binlog.currentFileOffset == oneSegmentLength

        when:
        // write to next file not created yet
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, oneFileMaxLength - oneSegmentLength)
        then:
        // create next file
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == 0

        when:
        oneSegmentBytes = new byte[oneSegmentLength / 2]
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, oneFileMaxLength - oneSegmentLength)
        then:
        // not change as current file index is bigger
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == 0

        when:
        binlog.moveToNextSegment()
        then:
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == 0

        when:
        binlog.currentFileOffset = 1
        binlog.moveToNextSegment()
        then:
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == oneSegmentLength

        when:
        binlog.currentFileOffset = oneFileMaxLength - oneSegmentLength + 1
        binlog.moveToNextSegment()
        then:
        binlog.currentFileIndex == 7
        binlog.currentFileOffset == 0

        when:
        exception = false
        oneSegmentBytes = new byte[oneSegmentLength + 1]
        try {
            binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        dynConfig.binlogOn = false
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, 0)
        then:
        1 == 1

        cleanup:
        println 'in memory size estimate: ' + binlog.estimate()
        binlog.clear()
        binlog.close()
        Consts.slotDir.deleteDir()
        slotDir2.deleteDir()
    }

    def 'test apply'() {
        given:
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
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def replPair = ReplPairTest.mockAsSlave()
        Binlog.decodeAndApply(slot, oneSegmentBytes, 0, replPair)
        then:
        oneSlot.getWalByBucketIndex(0).keyCount == 10

        when:
        def n = Binlog.decodeAndApply(slot, oneSegmentBytes, oneSegmentBytes.length, replPair)
        then:
        n == 0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
