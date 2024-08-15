package redis

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import redis.command.XGroup
import redis.decode.Request
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.Repl
import redis.repl.ReplType
import redis.reply.*
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.time.Duration

class RequestHandlerTest extends Specification {
    final byte slot = 0
    final byte workerId = 0
    final byte netWorkers = 1
    final short slotNumber = 1

    def 'test handle'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def requestHandler = new RequestHandler(workerId, netWorkers, slotNumber, snowFlake, Config.create())
        println requestHandler
        requestHandler.sampleToTrainSizeGauge.collect()

        def config2 = Config.create().with('localTest', 'true')
        def requestHandler2 = new RequestHandler(workerId, netWorkers, slotNumber, snowFlake, config2)

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def socket = TcpSocket.wrapChannel(eventloopCurrent, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        def oneSlot = localPersist.oneSlot(slot)

        expect:
        requestHandler.workerId == workerId
        requestHandler.netWorkers == netWorkers
        requestHandler.slotNumber == slotNumber
        requestHandler.snowFlake == snowFlake
        requestHandler.password == null
        requestHandler2.localTestRandomValueList.size() > 0

        when:
        def requestList = ('a'..'z').collect {
            def cmd = it + 'zzz'
            def data = new byte[1][]
            data[0] = cmd.bytes
            new Request(data, false, false)
        }
        def requestList2 = ('A'..'Z').collect {
            def cmd = it + 'zzz'
            def data = new byte[1][]
            data[0] = cmd.bytes
            new Request(data, false, false)
        }
        def someRequestList = ['ping', 'quit', 'auth'].collect {
            def data = new byte[1][]
            data[0] = it.bytes
            new Request(data, false, false)
        }
        def otherData = new byte[1][]
        otherData[0] = '123'.bytes
        def otherRequest = new Request(otherData, false, false)
        RequestHandler.parseSlots(otherRequest)
        then:
        requestList.every {
            RequestHandler.parseSlots(it)
            it.slotWithKeyHashList.size() == 0
        }
        requestList2.every {
            RequestHandler.parseSlots(it)
            it.slotWithKeyHashList.size() == 0
        }
        someRequestList.every {
            RequestHandler.parseSlots(it)
            it.slotWithKeyHashList == null
        }
        otherRequest.slotWithKeyHashList == null

        // test handle
        when:
        requestHandler.stop()
        then:
        requestHandler.handle(requestList[0], socket) == ErrorReply.SERVER_STOPPED

        when:
        requestHandler.isStopped = false
        then:
        requestList.every {
            requestHandler.handle(it, socket) == NilReply.INSTANCE
        }
        requestList2.every {
            requestHandler.handle(it, socket) == NilReply.INSTANCE
        }

        when:
        def reply = requestHandler.handle(someRequestList[0], socket)
        then:
        reply == PongReply.INSTANCE

        when:
        reply = requestHandler.handle(someRequestList[1], socket)
        then:
        reply == OKReply.INSTANCE

        when:
        reply = requestHandler.handle(someRequestList[2], socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        def authData = new byte[2][]
        authData[0] = 'auth'.bytes
        authData[1] = 'password'.bytes
        def authRequest = new Request(authData, false, false)
        reply = requestHandler.handle(authRequest, socket)
        then:
        reply == ErrorReply.NO_PASSWORD

        when:
        requestHandler.password = 'password1'
        reply = requestHandler.handle(authRequest, socket)
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        requestHandler.password = 'password'
        reply = requestHandler.handle(authRequest, socket)
        then:
        reply == OKReply.INSTANCE

        when:
        AfterAuthFlagHolder.remove(socket.remoteAddress)
        def getData1 = new byte[1][]
        getData1[0] = 'get'.bytes
        def getRequest = new Request(getData1, false, false)
        reply = requestHandler.handle(getRequest, socket)
        then:
        reply == ErrorReply.NO_AUTH

        when:
        AfterAuthFlagHolder.add(socket.remoteAddress)
        reply = requestHandler.handle(getRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        requestHandler.password = null
        reply = requestHandler.handle(getRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def key = 'key'
        def getData2 = new byte[2][]
        getData2[0] = 'get'.bytes
        getData2[1] = key.bytes
        def getRequest2 = new Request(getData2, false, false)
        getRequest2.slotNumber = slotNumber
        RequestHandler.parseSlots(getRequest2)
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply == NilReply.INSTANCE

        when:
        getData2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def sKey = BaseCommand.slot(key.bytes, slotNumber)
        def cv = new CompressedValue()
        cv.compressedData = new byte[10]
        cv.compressedLength = 10
        cv.uncompressedLength = 10
        cv.keyHash = sKey.keyHash()
        oneSlot.put(key, sKey.bucketIndex(), cv)
        getData2[1] = key.bytes
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 10

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cv.keyHash = sKey.keyHash()
        oneSlot.put(key, sKey.bucketIndex(), cv)
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof ErrorReply

        when:
        getData2[1] = XGroup.CONF_FOR_SLOT_KEY.bytes
        reply = requestHandler.handle(getRequest2, socket)
        then:
        reply instanceof BulkReply

        when:
        def setData3 = new byte[3][]
        setData3[0] = 'set'.bytes
        setData3[1] = key.bytes
        setData3[2] = 'value'.bytes
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())
        def setRequest = new Request(setData3, false, false)
        setRequest.slotNumber = slotNumber
        RequestHandler.parseSlots(setRequest)
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == OKReply.INSTANCE

        when:
        setData3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        setData3[1] = key.bytes
        setData3[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        oneSlot.readonly = true
        setData3[2] = 'value'.bytes
        reply = requestHandler.handle(setRequest, socket)
        then:
        reply == ErrorReply.READONLY

        when:
        oneSlot.readonly = false
        setData3[0] = '123'.bytes
        def setRequest2 = new Request(setData3, false, false)
        reply = requestHandler.handle(setRequest2, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        // repl
        def slaveUuid = 11L
        def replData = new byte[4][]
        replData[0] = new byte[8]
        ByteBuffer.wrap(replData[0]).putLong(slaveUuid)
        replData[1] = new byte[1]
        replData[1][0] = slot
        replData[2] = new byte[1]
        replData[2][0] = ReplType.ok.code
        replData[3] = new byte[0]
        oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, 'localhost', 6380)
        def replRequest = new Request(replData, false, true)
        replRequest.slotNumber = slotNumber
        reply = requestHandler.handle(replRequest, socket)
        then:
        reply instanceof Repl.ReplReply

        when:
        // http metrics
        def httpData = new byte[1][]
        httpData[0] = 'metrics'.bytes
        def httpRequest = new Request(httpData, true, false)
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw).contains('dict_size')

        when:
        httpData[0] = null
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        httpData[0] = '123'.bytes
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        Debug.instance.logCmd = true
        reply = requestHandler.handle(httpRequest, socket)
        then:
        reply == ErrorReply.FORMAT

        when:
        def httpData2 = new byte[2][]
        httpData2[0] = '123'.bytes
        httpData2[1] = '123'.bytes
        def httpRequest2 = new Request(httpData2, true, false)
        reply = requestHandler.handle(httpRequest2, socket)
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        Debug.instance.logCmd = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
