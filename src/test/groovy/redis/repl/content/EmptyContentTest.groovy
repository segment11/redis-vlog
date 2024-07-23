package redis.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

class EmptyContentTest extends Specification {
    def 'test all'() {
        given:
        def content = EmptyContent.INSTANCE

        expect:
        content.encodeLength() == 1

        when:
        def bytes = new byte[1]
        var buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        then:
        bytes[0] == 0
        EmptyContent.isEmpty(bytes)
    }
}
