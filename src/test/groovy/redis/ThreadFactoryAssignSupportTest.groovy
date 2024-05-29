package redis

import spock.lang.Specification

import java.util.concurrent.ThreadFactory

class ThreadFactoryAssignSupportTest extends Specification {
    def 'test share same core'() {
        given:
        def inner = ThreadFactoryAssignSupport.instance.ForMultiSlotRequest

        when:
        def n = 256

        List<ThreadFactory> list = []
        n.times {
            list << inner.getNextThreadFactory()
        }

        def m = inner.number * inner.threadNumberPerGroup

        then:
        (0..<inner.number).every { i ->
            list[i * inner.threadNumberPerGroup..(i + 1) * inner.threadNumberPerGroup - 1].every {
                it == list[i * inner.threadNumberPerGroup]
            }
        }

        (m..<n).every {
            def diff = it - m
            list[it] == list[diff]
        }
    }
}
