package redis.persist

import spock.lang.Specification

class BigStringFilesTest extends Specification {
    def 'test write and read'() {
        given:
        def bigString = 'a' * 1000000
        def bigStringFiles = new BigStringFiles((byte) 0, Consts.slotDir)

        when:
        bigStringFiles.writeBigStringBytes(1L, 'a', bigString.bytes)

        then:
        bigStringFiles.getBigStringBytesFromCache(1L) == bigString.bytes

        when:
        bigStringFiles.deleteBigStringFileIfExist(1L)

        then:
        bigStringFiles.getBigStringFileUuidList().size() == 0
    }
}
