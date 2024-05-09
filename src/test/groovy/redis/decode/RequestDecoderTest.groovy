package redis.decode

import io.activej.bytebuf.ByteBuf
import io.activej.bytebuf.ByteBufs
import redis.repl.Repl
import redis.repl.ReplType
import redis.repl.content.Ping
import spock.lang.Specification

import java.nio.ByteBuffer

class RequestDecoderTest extends Specification {
    def "TryDecode"() {
        given:
        def decoder = new RequestDecoder()

        and:
        def buf1 = ByteBuf.wrapForReading(
                ('*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n' +
                        '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n').bytes
        )
        def buf2 = ByteBuf.wrapForReading(
                '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'.bytes
        )

        def bufs = new ByteBufs(2)
        bufs.add(buf1)
        bufs.add(buf2)

        when:
        def requestList = decoder.tryDecode(bufs)

        then:
        requestList.size() == 3
        requestList.every { !it.isHttp() && !it.isRepl() }
    }

    def "TryDecodeHttp"() {
        given:
        def decoder = new RequestDecoder()

        and:
        def buf = ByteBuf.wrapForReading(
                "GET /?get&mykey HTTP/1.1\r\nHost: localhost:8080\r\nConnection: keep-alive\r\n\r\n".bytes
        )

        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        def requestList = decoder.tryDecode(bufs)

        then:
        requestList.size() == 1
        requestList[0].isHttp()

        requestList[0].data.length == 2
        requestList[0].data[0] == 'get'.bytes
        requestList[0].data[1] == 'mykey'.bytes
    }

    def "TryDecodeRepl"() {
        given:
        def decoder = new RequestDecoder()

        and:
        def ping = new Ping('127.0.0.1:7379')
        def buf = Repl.buffer(0L, (byte) 0, ReplType.ping, ping)

        def bufs = new ByteBufs(1)
        bufs.add(buf)

        when:
        def requestList = decoder.tryDecode(bufs)

        then:
        requestList.size() == 1
        requestList[0].isRepl()

        requestList[0].data.length == 4
        ByteBuffer.wrap(requestList[0].data[0]).getLong() == 0L
        requestList[0].data[1][0] == 0
        requestList[0].data[2][0] == ReplType.ping.code
        requestList[0].data[3].length == ping.encodeLength()
    }
}
