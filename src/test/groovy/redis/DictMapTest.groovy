package redis

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class DictMapTest extends Specification {

    def 'Init'() {
        given:
        def dictMap = DictMap.instance

        def dirFile = new File('/tmp/redis-vlog-test-dir')
        FileUtils.forceMkdir(dirFile)

        and:
        dictMap.initDictMap(dirFile)

        var dict = new Dict()
        dict.dictBytes = "test".getBytes()
        dict.seq = 1
        dict.createdTime = System.currentTimeMillis()

        when:
        dictMap.putDict("test", dict)
        then:
        dictMap.getDict("test").dictBytes == "test".getBytes()
        dictMap.getDictBySeq(1).dictBytes == "test".getBytes()

        cleanup:
//        compressHandler.clearAll();
        dictMap.close();
    }
}
