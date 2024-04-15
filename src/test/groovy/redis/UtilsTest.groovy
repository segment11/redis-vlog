package redis

import redis.stats.StatKV
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

    def "PadStats"() {
        given:
        List<StatKV> list = []
        list << new StatKV("abc", (double) 1)
        expect:
        Utils.padStats(list, 5) == "abc   1.0\n"

        when:
        list << StatKV.split
        list << new StatKV("xyz", (double) 2)
        then:
        Utils.padStats(list, 5) == "abc   1.0\n${StatKV.split.key()}\nxyz   2.0\n"
    }
}
