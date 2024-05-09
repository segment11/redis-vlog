package redis.decode

import spock.lang.Specification

class RESPTest extends Specification {
    def "Decode"() {
        given:
        def resp = new RESP()

        when:
        def getCmdStr = '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'
        def setCmdStr = '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n'

        def getCmdData = resp.decode(io.netty.buffer.Unpooled.wrappedBuffer(getCmdStr.bytes))
        def setCmdData = resp.decode(io.netty.buffer.Unpooled.wrappedBuffer(setCmdStr.bytes))

        then:
        getCmdData.length == 2
        getCmdData[0] == 'GET'.bytes
        getCmdData[1] == 'mykey'.bytes

        setCmdData.length == 3
        setCmdData[0] == 'SET'.bytes
        setCmdData[1] == 'mykey'.bytes
        setCmdData[2] == 'myvalue'.bytes
    }
}
