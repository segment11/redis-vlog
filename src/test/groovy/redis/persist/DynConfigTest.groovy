package redis.persist

import spock.lang.Specification

class DynConfigTest extends Specification {

    def "test update"() {
        given:
        def dynConfigFile = new File('/tmp/dyn-config.json')
        def config = new DynConfig((byte) 0, dynConfigFile)

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
        then:
        config.readonly
        !config.canRead

        // reload from file
        when:
        config = new DynConfig((byte) 0, dynConfigFile)
        then:
        config.masterUuid == 1234L
        config.testKey == 1
        config.readonly
        !config.canRead
    }
}
