package redis.decode

import redis.BaseCommand
import spock.lang.Specification

class RequestTest extends Specification {
    def 'test all'() {
        given:
        byte[][] data = new byte[3][]
        data[0] = 'SET'.bytes
        data[1] = 'key'.bytes
        data[2] = 'value'.bytes

        def request = new Request(data, false, false)
        request.slotNumber = 1

        expect:
        request.data.length == 3
        !request.isHttp()
        !request.isRepl()

        request.cmd() == 'set'
        request.singleSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER
        request.slotNumber == 1

        when:
        request.slotWithKeyHashList = [BaseCommand.slot('key'.bytes, 1)]

        then:
        request.slotWithKeyHashList.size() == 1
        request.singleSlot == request.slotWithKeyHashList[0].slot

        when:
        byte[][] data2 = new byte[3][]
        data2[0] = 'MGET'.bytes
        data2[1] = 'key1'.bytes
        data2[2] = 'key2'.bytes

        def request2 = new Request(data, true, false)
        request2.slotNumber = 2

        request2.slotWithKeyHashList = [
                BaseCommand.slot('key1'.bytes, 2),
                BaseCommand.slot('key2'.bytes, 2)
        ]

        def isMultiSlot = request2.slotWithKeyHashList.collect { it.slot }.unique().size() > 1
        request2.isCrossRequestWorker = isMultiSlot

        then:
        request2.isCrossRequestWorker == isMultiSlot
        request2.singleSlot == request2.slotWithKeyHashList[0].slot

        when:
        def data3 = new byte[1][]
        data3[0] = 'dbsize'.bytes

        def request3 = new Request(data3, false, false)
        request3.checkCmdIfCrossRequestWorker()

        then:
        request3.isCrossRequestWorker

        when:
        Request.crossRequestWorkerCmdList << 'xxxx'
        def data4 = new byte[1][]
        data4[0] = 'xxxx'.bytes

        def request4 = new Request(data4, false, false)
        request4.checkCmdIfCrossRequestWorker()

        then:
        request4.isCrossRequestWorker
    }
}
