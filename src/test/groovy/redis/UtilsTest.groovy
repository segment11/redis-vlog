package redis

import spock.lang.Specification

class UtilsTest extends Specification {
    def "RightPad"() {
        expect:
        Utils.rightPad("abc", 'x', 5) == "abcxx"
    }

    def "LeftPad"() {
        expect:
        Utils.leftPad("abc", 'x', 5) == "xxabc"
    }
}
