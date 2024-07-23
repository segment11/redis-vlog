package redis.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

import java.nio.ByteBuffer

class HelloTest extends Specification {
    def 'test all'() {
        given:
        def address = 'localhost:6380'
        def content = new Hello(11L, address)

        expect:
        content.encodeLength() == 8 + address.length()

        when:
        def bytes = new byte[content.encodeLength()]
        var buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getLong() == 11L
    }
}
