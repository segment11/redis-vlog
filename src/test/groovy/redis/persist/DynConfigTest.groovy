package redis.persist

import spock.lang.Specification

class DynConfigTest extends Specification {

    def 'test all'() {
        given:
        def dynConfigFile = new File('/tmp/dyn-config.json')
        if (dynConfigFile.exists()) {
            dynConfigFile.delete()
        }
        def config = new DynConfig((byte) 0, dynConfigFile)

        expect:
        config.masterUuid == null
        !config.readonly
        config.canRead
        config.canWrite
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
        config.readonly = true
        config.canRead = false
        config.canWrite = false
        then:
        config.readonly
        !config.canRead
        !config.canWrite

        // reload from file
        when:
        config = new DynConfig((byte) 0, dynConfigFile)
        then:
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
        dynConfigFile.delete()
    }
}
