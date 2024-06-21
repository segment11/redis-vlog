package redis

import spock.lang.Specification

class UtilsTest extends Specification {
    def "RightPad"() {
        expect:
        Utils.rightPad("abc", 'x', 5) == "abcxx"
        Utils.rightPad("abcde", 'x', 5) == "abcde"
    }

    def "LeftPad"() {
        expect:
        Utils.leftPad("abc", 'x', 5) == "xxabc"
        Utils.leftPad("abcde", 'x', 5) == "abcde"
    }
}
