package redis.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

class RawBytesContentTest extends Specification {
    def 'test all'() {
        given:
        def rawBytes = 'xxx'.bytes
        def content = new RawBytesContent(rawBytes)

        expect:
        content.encodeLength() == rawBytes.length

        when:
        def bytes = new byte[content.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        then:
        bytes == rawBytes
    }
}
