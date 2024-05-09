package redis.decode

import spock.lang.Specification

class HeaderBodyTest extends Specification {
    def "Feed"() {
        given:
        def h = new HttpHeaderBody()
        def h2 = new HttpHeaderBody()
        def h3 = new HttpHeaderBody()

        when:
        def getHeader = "GET / HTTP/1.1\r\nAccept: text/html\r\n\r\n".bytes
        h.feed(getHeader)
        then:
        h.isOk
        h.action == "GET / HTTP/1.1"
        h.httpVersion == "HTTP/1.1"
        h.requestType == "GET"
        h.url == "/"
        h.contentLength() == 0
        h.headers.size() == 1

        when:
        def postHeader = "POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n".bytes
        h2.feed(postHeader)
        then:
        !h2.isOk
        h2.action == "POST / HTTP/1.1"
        h2.httpVersion == "HTTP/1.1"
        h2.requestType == "POST"
        h2.url == "/"
        h2.contentLength() == 4
        h2.headers.size() == 1

        when:
        def fullHttpHeaderWithBody = "POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n1234".bytes
        h3.feed(fullHttpHeaderWithBody)
        then:
        h3.isOk
        h3.action == "POST / HTTP/1.1"
        h3.httpVersion == "HTTP/1.1"
        h3.requestType == "POST"
        h3.url == "/"
        h3.contentLength() == 4
        h3.headers.size() == 1
        '1234' == new String(h3.body())
    }
}
