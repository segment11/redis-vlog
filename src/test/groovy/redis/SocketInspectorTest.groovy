package redis

import io.activej.async.callback.AsyncComputation
import io.activej.common.function.SupplierEx
import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import redis.command.XGroup
import redis.repl.ReplPairTest
import redis.reply.BulkReply
import spock.lang.Specification

import java.nio.channels.SocketChannel
import java.time.Duration

class SocketInspectorTest extends Specification {
    def 'test connect'() {
        given:
        def inspector = new SocketInspector()
        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        when:
        inspector.onConnect(socket)
        inspector.onDisconnect(socket)
        inspector.subscribe('test_channel', socket)
        inspector.onDisconnect(socket)
        inspector.onReadTimeout(socket)
        inspector.onRead(socket, null)
        inspector.onReadEndOfStream(socket)
        inspector.onReadError(socket, null)
        inspector.onWriteTimeout(socket)
        inspector.onWrite(socket, null, 10)
        inspector.onWriteError(socket, null)
        then:
        inspector.lookup(SocketInspector.class) == null

        when:
        XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed = true
        socket.userData = ReplPairTest.mockAsSlave()
        inspector.onConnect(socket)
        inspector.onDisconnect(socket)
        then:
        1 == 1

        when:
        socket.userData = null
        inspector.maxConnections = 1
        println inspector.maxConnections
        boolean exception = false
        try {
            inspector.onConnect(socket)
            inspector.onConnect(socket)
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        inspector.clearAll()
    }

    def 'test subscribe'() {
        given:
        def inspector = new SocketInspector()
        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        and:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def eventloop2 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        eventloop2.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        Thread.start {
            eventloop2.run()
        }
        Thread.sleep(1000)

        inspector.netWorkerEventloopArray = [eventloop2, eventloop]

        when:
        def channel = 'test_channel'
        def channel2 = 'test_channel2'
        def messageReply = new BulkReply('test_message'.bytes)
        def n = inspector.publish(channel, messageReply, (s, r) -> { })
        then:
        n == 0
        inspector.subscribeSocketCount(channel) == 0

        when:
        n = inspector.subscribe(channel, socket)
        then:
        n == 1
        inspector.subscribeSocketCount(channel) == 1

        when:
        // in eventloop thread
        SupplierEx<Integer> supplierEx = () -> inspector.subscribe(channel2, socket)
        eventloop.submit(AsyncComputation.of(supplierEx)).get()
        then:
        inspector.subscribeSocketCount(channel2) == 1

        when:
        n = inspector.unsubscribe(channel, socket)
        then:
        n == 0

        when:
        n = inspector.publish(channel, messageReply, (s, r) -> { })
        def n2 = inspector.publish(channel2, messageReply, (s, r) -> {
            println 'async callback to write message to target socket'
        })
        then:
        n == 0
        n2 == 1

        when:
        inspector.subscribe(channel, socket)
        n = inspector.publish(channel, messageReply, (s, r) -> { })
        then:
        n == 1

        cleanup:
        Thread.sleep(1000)
        eventloop.breakEventloop()
        eventloop2.breakEventloop()
        inspector.clearAll()
    }
}
