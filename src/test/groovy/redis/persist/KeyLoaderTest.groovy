package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.SnowFlake
import spock.lang.Specification

class KeyLoaderTest extends Specification {
    private static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    def 'test write and read one key'() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')

        def snowFlake = new SnowFlake(1, 1)

        byte slot = 0
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, null, null)
        keyLoader.init(libC)
        keyLoader.initEventloop()
        keyLoader.initAfterEventloopReady()

        when:
        keyLoader.putValueByKeyForTest(0, 'a'.getBytes(), 1L, 0L, 'a'.bytes)
        def valueBytesWithExpireAt = keyLoader.getValueByKey(0, 'a'.bytes, 1L)

        then:
        valueBytesWithExpireAt.valueBytes() == 'a'.bytes

        cleanup:
        keyLoader.cleanUp()
    }
}
