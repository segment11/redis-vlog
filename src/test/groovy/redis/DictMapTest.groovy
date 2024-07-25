package redis

import org.apache.commons.io.FileUtils
import redis.persist.Consts
import redis.persist.DynConfig
import redis.persist.DynConfigTest
import redis.repl.Binlog
import spock.lang.Specification

class DictMapTest extends Specification {
    def 'test all'() {
        given:
        def dirFile = new File('/tmp/redis-vlog-test-dir')
        FileUtils.forceMkdir(dirFile)

        def dictFile = new File(dirFile, 'dict-map.dat')
        if (dictFile.exists()) {
            dictFile.delete()
        }

        and:
        def dictMap = DictMap.instance
        dictMap.close()
        dictMap.initDictMap(dirFile)

        and:
        final byte slot = 0
        def dynConfig = new DynConfig(slot, DynConfigTest.tmpFile)
        def binlog = new Binlog(slot, Consts.slotDir, dynConfig)
        dictMap.binlog = binlog

        def dict = new Dict()
        dict.dictBytes = 'test'.bytes
        dict.seq = 1
        dict.createdTime = System.currentTimeMillis()

        def dict2 = new Dict()
        dict2.dictBytes = 'test2'.bytes
        dict2.seq = 0
        dict2.createdTime = System.currentTimeMillis()

        expect:
        dictMap.getCacheDictCopy().size() == 0
        dictMap.getCacheDictBySeqCopy().size() == 0

        when:
        dictMap.putDict('test', dict)
        // seq conflict, will reset seq
        dictMap.putDict('test', dict)
        dictMap.binlog = null
        dictMap.putDict('test2', dict2)
        then:
        dictMap.dictSize() == 3
        dictMap.getDict('test').dictBytes == 'test'.bytes
        dictMap.getDict('test2').dictBytes == 'test2'.bytes
        dictMap.getDictBySeq(1).dictBytes == 'test'.bytes
        dictMap.getDictBySeq(dict.seq).dictBytes == 'test'.bytes
        dictMap.getDictBySeq(0).dictBytes == 'test2'.bytes

        when:
        // reload again
        dictMap.initDictMap(dirFile)
        then:
        dictMap.dictSize() == 3

        when:
        dictMap.clearAll()
        dictMap.close()

        boolean exception = false
        try {
            dictMap.putDict('test', dict)
        } catch (RuntimeException e) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            dictMap.clearAll()
        } catch (RuntimeException e) {
            exception = true
        }
        then:
        exception

        cleanup:
        binlog.clear()
        binlog.close()
        Consts.slotDir.deleteDir()
    }
}
