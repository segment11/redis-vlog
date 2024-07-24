package redis.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

import java.nio.ByteBuffer

class ToMasterCatchUpForBinlogOneSegmentTest extends Specification {
    def 'test all'() {
        given:
        def content = new ToMasterCatchUpForBinlogOneSegment(10L, 0, 0L)

        expect:
        content.encodeLength() == 20

        when:
        def bytes = new byte[content.encodeLength()]
        var buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getLong() == 10L
        buffer.getInt() == 0
        buffer.getLong() == 0L
    }
}
