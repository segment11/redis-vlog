package redis.repl

import spock.lang.Specification

class ReplPairTest extends Specification {
    static ReplPair mockOne(byte slot = 0, boolean asMaster = true, String host = 'localhost', int port = 6379) {
        def replPair = new ReplPair(slot, asMaster, host, port)
        replPair
    }

    static ReplPair mockAsMaster(long masterUuid = 0L) {
        def replPair = mockOne()
        replPair.masterUuid = masterUuid
        replPair
    }

    static ReplPair mockAsSlave(long masterUuid = 0L, long slaveUuid = 1L) {
        def replPair = mockOne((byte) 0, false, 'localhost', 6380)
        replPair.masterUuid = masterUuid
        replPair.slaveUuid = slaveUuid
        replPair
    }

    def 'test base'() {
        given:
        final byte slot = 0

        def replPair = mockAsMaster()
        def replPair2 = mockAsSlave()

        expect:
        replPair.slot == slot
        replPair.host == 'localhost'
        replPair.port == 6379
        replPair.asMaster
        replPair.masterUuid == 0L
        replPair.lastPingGetTimestamp == 0L
        !replPair.sendBye

        replPair2.slot == slot
        replPair2.host == 'localhost'
        replPair2.port == 6380
        !replPair2.asMaster
        replPair2.masterUuid == 0L
        replPair2.slaveUuid == 1L
        replPair2.lastPongGetTimestamp == 0L

        def replPair1 = mockOne(slot, true, 'localhost', 16379)
        def replPair11 = mockOne(slot, true, 'local', 6379)

        replPair == replPair
        replPair != null
        replPair != new Object()
        replPair != replPair1
        replPair != replPair11
        replPair != replPair2

        when:
        replPair.lastPingGetTimestamp = System.currentTimeMillis() - 1000L
        replPair2.lastPongGetTimestamp = System.currentTimeMillis() - 2000L

        then:
        !replPair.linkUp
        !replPair2.linkUp
    }
}
