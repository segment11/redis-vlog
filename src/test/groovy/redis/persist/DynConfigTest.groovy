package redis.persist

import redis.MultiWorkerServer
import redis.SocketInspector
import spock.lang.Specification

class DynConfigTest extends Specification {
    final byte slot = 0

    static File tmpFile = new File('/tmp/dyn-config.json')
    static File tmpFile2 = new File('/tmp/dyn-config2.json')

    def 'test all'() {
        given:
        if (tmpFile.exists()) {
            tmpFile.delete()
        }
        def config = new DynConfig(slot, tmpFile)

        expect:
        config.masterUuid == null
        !config.readonly
        config.canRead
        config.canWrite
        !config.binlogOn
        config.testKey == 10

        when:
        config.masterUuid = 1234L
        then:
        config.masterUuid == 1234L

        when:
        config.testKey = 1
        then:
        config.testKey == 1

        when:
        config.binlogOn = false
        then:
        !config.binlogOn

        when:
        config.binlogOn = true
        then:
        config.binlogOn

        when:
        config.readonly = true
        config.canRead = false
        config.canWrite = false
        then:
        config.readonly
        !config.canRead
        !config.canWrite

        // reload from file
        when:
        MultiWorkerServer.staticGlobalV.socketInspector = new SocketInspector()
        config = new DynConfig(slot, tmpFile)
        config.update('max_connections', 100)
        then:
        config.afterUpdateCallback != null
        config.masterUuid == 1234L
        config.testKey == 1
        config.readonly
        !config.canRead
        !config.canWrite

        when:
        config.readonly = false
        config.canRead = true
        config.canWrite = true
        then:
        !config.readonly
        config.canRead
        config.canWrite

        cleanup:
        tmpFile.delete()
        tmpFile2.delete()
    }
}
