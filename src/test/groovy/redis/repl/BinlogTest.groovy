package redis.repl

import redis.ConfForSlot
import redis.persist.Consts
import redis.persist.Mock
import redis.persist.Wal
import spock.lang.Specification

class BinlogTest extends Specification {
    class ForWal implements BinlogAppendContent<ForWal> {
        private final Wal.V v

        ForWal(Wal.V v) {
            this.v = v
        }

        @Override
        Binlog.Type type() {
            Binlog.Type.wal
        }

        @Override
        byte[] encode() {
            v.encode()
        }

        @Override
        ForWal decode(byte[] bytes) {
            Wal.V.decode(new DataInputStream(new ByteArrayInputStream(bytes)))
        }
    }

    def 'test append'() {
        given:
        final byte slot = 0
        ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount = 2

        println 'Binlog types: ' + Binlog.Type.values().collect { it.name() }.join(', ')
        println new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)

        expect:
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 1) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0)

        when:
        def binlog = new Binlog(slot, Consts.slotDir)

        final File slotDir2 = new File('/tmp/redis-vlog/test-persist/test-slot2')
        if (!slotDir2.exists()) {
            slotDir2.mkdir()
            def binlogDir2 = new File(slotDir2, 'binlog')
            if (!binlogDir2.exists()) {
                binlogDir2.mkdir()
            }
            new File(binlogDir2, 'test.txt').text = 'test'
        }
        def binlog2 = new Binlog(slot, slotDir2)

        and:
        def vList = Mock.prepareValueList(11)
        for (v in vList[0..9]) {
            binlog.append(new ForWal(v))
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
            binlog.append(new ForWal(v))
        }
        then:
        binlog.currentFileIndex == 1
        binlog.prevRaf(0) != null
        binlog.readPrevRafOneSegment(0, 0).length == oneSegmentLength

        when:
        binlog.currentFileOffset = oneSegmentLength - 1
        for (v in vList[0..9]) {
            binlog.append(new ForWal(v))
        }
        binlog.currentFileOffset = oneSegmentLength * 2 - 1
        for (v in vList[0..9]) {
            binlog.append(new ForWal(v))
        }
        binlog.currentFileOffset = oneSegmentLength * 3 - 1
        for (v in vList[0..9]) {
            binlog.append(new ForWal(v))
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
        binlog.readCurrentRafOneSegment(oneSegmentLength * 3).length == vList[0].encodeLength() * 10

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
        binlog = new Binlog(slot, Consts.slotDir)
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
}
