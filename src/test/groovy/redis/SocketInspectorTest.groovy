package redis

import io.activej.net.socket.tcp.TcpSocket
import spock.lang.Specification

import java.nio.channels.SocketChannel

class SocketInspectorTest extends Specification {
    def 'test all'() {
        given:
        def inspector = new SocketInspector()
        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        when:
        inspector.onConnect(socket)
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

        cleanup:
        inspector.clearAll()
    }
}
