package redis

import io.activej.net.socket.tcp.TcpSocket
import redis.reply.BulkReply
import spock.lang.Specification

import java.nio.channels.SocketChannel

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

        when:
        def channel = 'test_channel'
        def n = inspector.publish(channel, new BulkReply('test_message'.bytes), false)
        then:
        n == 0
        inspector.subscribeSocketCount(channel) == 0

        when:
        n = inspector.subscribe(channel, socket)
        then:
        n == 1
        inspector.subscribeSocketCount(channel) == 1

        when:
        n = inspector.unsubscribe(channel, socket)
        then:
        n == 0

        when:
        n = inspector.publish(channel, new BulkReply('test_message'.bytes), false)
        then:
        n == 0

        when:
        inspector.subscribe(channel, socket)
        n = inspector.publish(channel, new BulkReply('test_message'.bytes), false)
        then:
        n == 1

        cleanup:
        inspector.clearAll()
    }
}
