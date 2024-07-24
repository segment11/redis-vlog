package redis.repl.content

import io.activej.bytebuf.ByteBuf
import redis.repl.Binlog
import spock.lang.Specification

import java.nio.ByteBuffer

class HiTest extends Specification {
    def 'test all'() {
        given:
        def content = new Hi(11L, 10L,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L))

        expect:
        content.encodeLength() == 40

        when:
        def bytes = new byte[content.encodeLength()]
        var buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getLong() == 11L
        buffer.getLong() == 10L
        buffer.getInt() == 1
        buffer.getLong() == 1
        buffer.getInt() == 0
        buffer.getLong() == 0

        when:
        def content2 = new Hi(11L, 10L,
                new Binlog.FileIndexAndOffset(1, 1L),
                null)
        def bytes2 = new byte[content2.encodeLength()]
        def buf2 = ByteBuf.wrapForWriting(bytes2)
        content2.encodeTo(buf2)
        def buffer2 = ByteBuffer.wrap(bytes2)
        then:
        buffer2.getLong() == 11L
        buffer2.getLong() == 10L
        buffer2.getInt() == 1
        buffer2.getLong() == 1
        buffer2.getInt() == -1
        buffer2.getLong() == -1
    }
}
