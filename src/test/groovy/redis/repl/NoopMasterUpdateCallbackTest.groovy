package redis.repl

import redis.Dict
import redis.persist.Mock
import spock.lang.Specification

class NoopMasterUpdateCallbackTest extends Specification {
    def 'test all'() {
        given:
        final byte slot = 0

        def callback = new NoopMasterUpdateCallback()

        when:
        def v = Mock.prepareValueList(1)[0]
        callback.onWalAppend(slot, 0, false, v, 0)
        callback.walAppendCount = 100_000 - 1
        callback.onWalAppend(slot, 0, false, v, 0)
        callback.onDictCreate('key:', new Dict())
        callback.flushToSlaveWalAppendBatch()
        callback.onBigStringFileWrite(slot, 0L, new byte[10])
        callback.bigStringFileWriteCount = 100 - 1
        callback.onBigStringFileWrite(slot, 0L, new byte[10])
        then:
        !callback.toSlaveWalAppendBatchEmpty
    }
}
