package redis.repl

import io.activej.config.Config
import io.activej.csp.binary.BinaryChannelSupplier
import io.activej.csp.consumer.ChannelConsumers
import io.activej.csp.supplier.ChannelSuppliers
import io.activej.eventloop.Eventloop
import io.activej.net.SimpleServer
import redis.ConfForSlot
import redis.RequestHandler
import redis.decode.RequestDecoder
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.content.RawBytesContent
import spock.lang.Specification

import java.time.Duration

class TcpClientTest extends Specification {
    def 'test connect and close'() {
        given:
        byte slot = 0
        ConfForSlot.global.netListenAddresses = 'localhost:6380'

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        def requestHandler = new RequestHandler(slot, (byte) 1, (short) 1, null, null, Config.create())
        def replPair2 = ReplPairTest.mockAsSlave()
        def tcpClient = new TcpClient(slot, eventloopCurrent, requestHandler, replPair2)

        expect:
        !tcpClient.isSocketConnected()

        when:
        tcpClient.write(ReplType.ok, new RawBytesContent('test'.bytes))
        then:
        tcpClient.notConnectedErrorCount == 1

        when:
        tcpClient.notConnectedErrorCount = 999
        tcpClient.ping()
        then:
        tcpClient.notConnectedErrorCount == 1000

        when:
        tcpClient.bye()
        then:
        tcpClient.notConnectedErrorCount == 1001

        when:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance

        tcpClient.close()
        println 'tcp client ready to reconnect'
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        def server = SimpleServer.builder(
                eventloop,
                socket -> {
                    println 'Client connected'
                    BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                            .decodeStream(new RequestDecoder())
                            .map { pipeline ->
                                def request = pipeline[0]
                                def data = request.getData()
                                println 'Mock server get request from client, data.length: ' + data.length
                                Repl.ok(slot, replPair2, 'ok').buffer()
                            }.streamTo(ChannelConsumers.ofSocket(socket))
                })
                .withListenAddress(new InetSocketAddress('localhost', 6379))
                .withAcceptOnce()
                .build()
        Thread.start {
            localPersist.fixSlotThreadId((byte) 0, Thread.currentThread().threadId())
            eventloop.run()
        }
        Thread.start {
            Thread.sleep(1000 * 2)
            eventloop.breakEventloop()
        }
        server.listen()
        tcpClient.connect('localhost', 6379) {
            tcpClient.write(ReplType.ok, new RawBytesContent('test'.bytes))
            println 'Send ok to server after client connected'
            Repl.ok(slot, replPair2, 'ok').buffer()
        }
        println 'before current eventloop run'
        eventloopCurrent.delay(1000, () -> {
            tcpClient.close()
        })
        eventloopCurrent.run()
        println 'after current eventloop run'
        then:
        1 == 1

        cleanup:
        // only for coverage
        tcpClient.close()
        localPersist.fixSlotThreadId((byte) 0, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
