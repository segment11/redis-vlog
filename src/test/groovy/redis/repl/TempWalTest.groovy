package redis.repl

import redis.ConfForSlot
import redis.persist.Consts
import redis.persist.Mock
import spock.lang.Specification

class TempWalTest extends Specification {

    def 'test read and append'() {
        given:
        final byte slot = 0
        ConfForSlot.global.confRepl.tempWalForReadCacheSegmentMaxCount = 2

        def tempWal = new TempWal(slot, Consts.slotDir)

        when:
        def vList = Mock.prepareValueList(11)
        for (v in vList[0..9]) {
            tempWal.append(v)
        }
        println 'temp wal meta: ' + tempWal.getMetaFileIndexAndOffset()
        println new TempWal.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        then:
        tempWal.getMetaFileIndexAndOffset().offset() == 0
        tempWal.prevRaf(-1) == null
        tempWal.readPrevRafOneSegment(-1, 0) == null
        tempWal.readPrevRafOneSegment(1, 0) == null
        tempWal.size() == 10
        tempWal.getCvEncoded(vList[0].key()).length == vList[0].cvEncoded().length
        tempWal.getCvEncoded(vList[10].key()) == null

        when:
        def oneFileMaxLength = ConfForSlot.global.confRepl.tempWalOneFileMaxLength
        def oneSegmentLength = ConfForSlot.global.confRepl.tempWalOneSegmentLength
        tempWal.currentFileOffset = oneFileMaxLength - 1
        for (v in vList[0..9]) {
            tempWal.append(v)
        }
        then:
        tempWal.currentFileIndex == 1
        tempWal.prevRaf(0) != null
        tempWal.readPrevRafOneSegment(0, 0).length == oneSegmentLength

        when:
        tempWal.currentFileOffset = oneSegmentLength - 1
        for (v in vList[0..9]) {
            tempWal.append(v)
        }
        tempWal.currentFileOffset = oneSegmentLength * 2 - 1
        for (v in vList[0..9]) {
            tempWal.append(v)
        }
        tempWal.currentFileOffset = oneSegmentLength * 3 - 1
        for (v in vList[0..9]) {
            tempWal.append(v)
        }
        then:
        tempWal.size() == 10
        tempWal.readCurrentRafOneSegment(0).length == oneSegmentLength
        tempWal.readCurrentRafOneSegment(oneSegmentLength).length == oneSegmentLength
        tempWal.readCurrentRafOneSegment(oneSegmentLength * 2).length == oneSegmentLength
        tempWal.readCurrentRafOneSegment(oneSegmentLength * 3).length == oneSegmentLength
        tempWal.readCurrentRafOneSegment(tempWal.currentFileOffset) == null

        when:
        tempWal.currentFileOffset = oneSegmentLength * 4
        then:
        tempWal.readCurrentRafOneSegment(oneSegmentLength * 3).length == vList[0].encodeLength() * 10

        when:
        tempWal.close()
        // load again
        tempWal.updateMetaFileIndexAndOffset(tempWal.currentFileIndex, tempWal.currentFileOffset - oneSegmentLength)
        then:
        def tempWal2 = new TempWal(slot, Consts.slotDir)
        then:
        tempWal2.size() == 10

        when:
        tempWal2.close()
        tempWal2.updateMetaFileIndexAndOffset(tempWal.currentFileIndex, oneSegmentLength)
        then:
        def tempWal3 = new TempWal(slot, Consts.slotDir)
        then:
        tempWal3.size() == 10

        when:
        def test0V = new TempWal.OffsetV(tempWal3.currentFileIndex - 1, 0, vList[0])
        println 'test 0 offset v: ' + test0V
        tempWal3.addForTest('test0', test0V)
        tempWal3.addForTest('test1', new TempWal.OffsetV(tempWal3.currentFileIndex, tempWal3.currentFileOffset - 1, vList[0]))
        tempWal3.addForTest('test2', new TempWal.OffsetV(tempWal3.currentFileIndex, tempWal3.currentFileOffset + 1, vList[0]))
        tempWal3.addForTest('test3', new TempWal.OffsetV(tempWal3.currentFileIndex + 1, 0, vList[0]))

        tempWal3.removeAfterCatchUp(tempWal3.currentFileIndex, tempWal3.currentFileOffset) {
            println 'removed from temp wal and need to write to wal (sst), list size: ' + it.size()
        }
        then:
        tempWal3.getMetaFileIndexAndOffset().offset() == tempWal3.currentFileOffset
        tempWal3.size() == 2
        tempWal3.getCvEncoded('test2') != null
        tempWal3.getCvEncoded('test3') != null
        // for clear
        tempWal3.prevRaf(tempWal3.currentFileIndex - 1) != null

        when:
        boolean exception = false
        try {
            tempWal3.readPrevRafOneSegment(0, 1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            tempWal3.readCurrentRafOneSegment(1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        cleanup:
        tempWal3.clear()
        tempWal3.close()
        Consts.slotDir.deleteDir()
    }
}
