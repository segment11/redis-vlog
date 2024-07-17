package redis.decode

import redis.BaseCommand
import spock.lang.Specification

class RequestTest extends Specification {
    def 'test all'() {
        given:
        def data3 = new byte[3][]
        data3[0] = 'SET'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'value'.bytes

        def request = new Request(data3, false, false)
        request.slotNumber = 1

        expect:
        request.data.length == 3
        !request.isHttp()
        !request.isRepl()

        request.cmd() == 'set'
        request.cmd() == 'set'
        request.singleSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER
        request.slotNumber == 1

        println request.toString()

        when:
        request.slotWithKeyHashList = [BaseCommand.slot('key'.bytes, 1)]

        then:
        request.slotWithKeyHashList.size() == 1
        request.singleSlot == request.slotWithKeyHashList[0].slot

        when:
        request.slotWithKeyHashList = []

        then:
        request.singleSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER

        when:
        def dataRepl = new byte[3][]
        dataRepl[0] = new byte[8]
        dataRepl[1] = new byte[1]
        dataRepl[1][0] = (byte) 0

        def requestRepl = new Request(dataRepl, false, true)

        then:
        requestRepl.isRepl()
        requestRepl.singleSlot == 0

        when:
        data3[0] = 'MGET'.bytes
        data3[1] = 'key1'.bytes
        data3[2] = 'key2'.bytes

        def request2 = new Request(data3, true, false)
        request2.slotNumber = 2

        request2.slotWithKeyHashList = [
                BaseCommand.slot('key1'.bytes, 2),
                BaseCommand.slot('key2'.bytes, 2)
        ]

        def isMultiSlot = request2.slotWithKeyHashList.collect { it.slot }.unique().size() > 1
        request2.setCrossRequestWorker(isMultiSlot)

        then:
        request2.isCrossRequestWorker() == isMultiSlot
        request2.singleSlot == request2.slotWithKeyHashList[0].slot

        when:
        def data1 = new byte[1][]
        data1[0] = 'dbsize'.bytes

        def request3 = new Request(data1, false, false)
        request3.checkCmdIfCrossRequestWorker()

        then:
        request3.isCrossRequestWorker()

        when:
        data1[0] = 'xxxx'.bytes
        def request33 = new Request(data1, false, false)
        request33.checkCmdIfCrossRequestWorker()

        then:
        !request33.isCrossRequestWorker()

        when:
        Request.crossRequestWorkerCmdList << 'xxxx'
        data1[0] = 'xxxx'.bytes

        def request4 = new Request(data1, false, false)
        request4.checkCmdIfCrossRequestWorker()

        then:
        request4.isCrossRequestWorker()
    }
}
