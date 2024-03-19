package redis

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class DictMapTest extends Specification {

    def 'Init'() {
        given:
        def compressHandler = DictMap.instance

        def dirFile = new File('/tmp/redis-vlog-test-dir')
        FileUtils.forceMkdir(dirFile)

        and:
        compressHandler.initDictMap(dirFile)

        var dict = new Dict()
        dict.dictBytes = "test".getBytes()
        dict.seq = 1
        dict.createdTime = System.currentTimeMillis()

        when:
        compressHandler.putDict("test", dict)
        then:
        compressHandler.getDict("test").dictBytes == "test".getBytes()
        compressHandler.getDictBySeq(1).dictBytes == "test".getBytes()

        cleanup:
//        compressHandler.clearAll();
        compressHandler.close();
    }
}
