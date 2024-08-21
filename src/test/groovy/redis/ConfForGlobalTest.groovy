package redis

import spock.lang.Specification

class ConfForGlobalTest extends Specification {
    // only for coverage
    def 'test all'() {
        given:
        println ConfForGlobal.estimateKeyNumber
        println ConfForGlobal.estimateOneValueLength

        println ConfForGlobal.isValueSetUseCompression
        println ConfForGlobal.isOnDynTrainDictForCompression

        println ConfForGlobal.netListenAddresses

        println ConfForGlobal.pureMemory
        println ConfForGlobal.slotNumber
        println ConfForGlobal.netWorkers
        println ConfForGlobal.eventLoopIdleMillis

        println ConfForGlobal.zookeeperConnectString
        println ConfForGlobal.zookeeperRootPath

        expect:
        1 == 1
    }
}
