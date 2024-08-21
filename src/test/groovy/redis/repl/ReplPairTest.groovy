package redis.repl

import io.activej.config.Config
import io.activej.csp.binary.BinaryChannelSupplier
import io.activej.csp.consumer.ChannelConsumers
import io.activej.csp.supplier.ChannelSuppliers
import io.activej.eventloop.Eventloop
import io.activej.net.SimpleServer
import redis.ConfForGlobal
import redis.RequestHandler
import redis.decode.RequestDecoder
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.content.RawBytesContent
import spock.lang.Specification

import java.time.Duration

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

    final byte slot = 0

    def 'test base'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6380'

        def replPairAsMaster = mockAsMaster()
        def replPairAsSlave = mockAsSlave()
        for (replType in ReplType.values()) {
            replPairAsMaster.increaseStatsCountForReplType(replType)
            replPairAsSlave.increaseStatsCountForReplType(replType)
        }

        // trigger log
        100.times {
            replPairAsMaster.increaseStatsCountForReplType(ReplType.pong)
            replPairAsMaster.increaseStatsCountForReplType(ReplType.catch_up)
            replPairAsSlave.increaseStatsCountForReplType(ReplType.ping)
            replPairAsSlave.increaseStatsCountForReplType(ReplType.s_catch_up)
        }

        println replPairAsMaster.statsCountForReplTypeAsString
        println replPairAsSlave.statsCountForReplTypeAsString

        replPairAsSlave.slaveCatchUpLastSeq = 1000L

        expect:
        replPairAsMaster.slot == slot
        replPairAsMaster.hostAndPort == 'localhost:6379'
        replPairAsMaster.asMaster
        replPairAsMaster.masterUuid == 0L
        replPairAsMaster.lastPingGetTimestamp == 0L
        replPairAsSlave.slaveCatchUpLastSeq == 1000L
        !replPairAsMaster.sendBye
        !replPairAsMaster.ping()
        !replPairAsMaster.bye()
        !replPairAsMaster.write(ReplType.ping, null)
        !replPairAsMaster.toFetchBigStringUuidList
        !replPairAsMaster.doFetchingBigStringUuidList
        replPairAsMaster.doingFetchBigStringUuid() == -1

        replPairAsSlave.slot == slot
        replPairAsSlave.hostAndPort == 'localhost:6380'
        !replPairAsSlave.asMaster
        replPairAsSlave.masterUuid == 0L
        replPairAsSlave.slaveUuid == 1L
        replPairAsSlave.lastPongGetTimestamp == 0L

        def replPairMaster1 = mockOne(slot, true, 'localhost', 16379)
        def replPairAsMaster11 = mockOne(slot, true, 'local', 6379)

        !replPairAsMaster.equals(null)
        !replPairAsMaster.equals(Integer.valueOf(0))
        replPairAsMaster != replPairAsSlave
        replPairAsMaster != replPairMaster1
        replPairAsMaster != replPairAsMaster11
        replPairAsMaster.equals(replPairAsMaster)
        !replPairAsMaster.linkUp

        when:
        replPairAsMaster.isSendBye = true
        replPairAsMaster.sendByeForTest = true
        then:
        !replPairAsMaster.ping()
        !replPairAsMaster.write(ReplType.ping, null)

        when:
        replPairAsMaster.lastPingGetTimestamp = System.currentTimeMillis() - 1000L
        replPairAsSlave.lastPongGetTimestamp = System.currentTimeMillis() - 2000L
        then:
        replPairAsMaster.linkUp
        !replPairAsSlave.linkUp

        when:
        replPairAsMaster.addToFetchBigStringUuid(1L)
        then:
        replPairAsMaster.doingFetchBigStringUuid() == 1L
        replPairAsMaster.doFetchingBigStringUuidList[0] == 1L

        when:
        replPairAsMaster.doneFetchBigStringUuid(100L)
        replPairAsMaster.doneFetchBigStringUuid(1L)
        then:
        replPairAsMaster.doFetchingBigStringUuidList.size() == 0

        when:
        def millis = System.currentTimeMillis() - 1000L
        replPairAsSlave.lastGetCatchUpResponseMillis = millis
        then:
        replPairAsSlave.lastGetCatchUpResponseMillis == millis

        when:
        replPairAsSlave.initAsSlave(null, null)
        then:
        !replPairAsSlave.isLinkUp()

        when:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def server = SimpleServer.builder(
                eventloopCurrent,
                socket -> {
                    println 'Client connected'
                    BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                            .decodeStream(new RequestDecoder())
                            .map { pipeline ->
                                def request = pipeline[0]
                                def data = request.getData()
                                println 'Mock server get request from client, data.length: ' + data.length
                                Repl.ok(slot, replPairAsSlave, 'ok').buffer()
                            }.streamTo(ChannelConsumers.ofSocket(socket))
                })
                .withListenAddress(new InetSocketAddress('localhost', 6380))
                .withAcceptOnce()
                .build()
        Thread.sleep(100)
        server.listen()
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
        replPairAsSlave.initAsSlave(eventloopCurrent, requestHandler)
        boolean[] isLinkUpArray = [false]
        replPairAsSlave.lastPongGetTimestamp = System.currentTimeMillis()
        replPairAsSlave.sendByeForTest = false
        println 'before current eventloop run'
        eventloopCurrent.delay(2000, () -> {
            isLinkUpArray[0] = replPairAsSlave.isLinkUp()
            replPairAsSlave.initAsSlave(eventloopCurrent, requestHandler)

            replPairAsSlave.ping()
            replPairAsSlave.write(ReplType.ok, new RawBytesContent('test'.bytes))
            replPairAsSlave.bye()
            Thread.sleep(100)
            replPairAsSlave.close()
        })
        eventloopCurrent.run()
        println 'after current eventloop run'
        then:
        isLinkUpArray[0]

        cleanup:
        replPairAsMaster.close()
        replPairAsSlave.close()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
