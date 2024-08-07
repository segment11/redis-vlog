package redis

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.activej.inject.binding.OptionalDependency
import io.activej.net.socket.tcp.TcpSocket
import redis.decode.Request
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.reply.BulkReply
import redis.reply.ErrorReply
import redis.reply.NilReply
import redis.task.TaskRunnable
import spock.lang.Specification

import java.nio.channels.SocketChannel
import java.time.Duration

class MultiWorkerServerTest extends Specification {
    final byte slot0 = 0
    final byte slot1 = 1
    final byte workerId0 = 0
    final byte workerId1 = 1
    final byte netWorkers = 2
    final short slotNumber = 2

    def 'test mock inject and handle'() {
        // only for coverage
        given:
        def dirFile = new File('/tmp/redis-vlog')
        def config = Config.create()
                .with('slotNumber', slotNumber.toString())
                .with('netWorkers', netWorkers.toString())

        dirFile.deleteDir()

        def m = new MultiWorkerServer()
        m.netWorkerEventloopArray = new Eventloop[2]
        m.requestHandlerArray = new RequestHandler[2]
        m.scheduleRunnableArray = new TaskRunnable[2]
        def dirFile2 = m.dirFile(config)
        def dirFile3 = m.dirFile(config)

        expect:
        dirFile2.exists()
        dirFile3.exists()
        m.staticGlobalV.socketInspector == null
        def pr = m.primaryReactor(config) != null
        m.workerReactor(workerId0, OptionalDependency.empty(), config) != null
        // need use activej inject mock, todo
        m.workerPool(null, config) == null
        m.config() != null
        m.snowFlakes(config) != null
        m.getModule() != null
        m.getBusinessLogicModule() != null

        when:
        def httpReply = m.wrapHttpResponse(new BulkReply('xxx'.bytes))
        def httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('200')

        when:
        httpReply = m.wrapHttpResponse(ErrorReply.FORMAT)
        httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('500')

        when:
        httpReply = m.wrapHttpResponse(NilReply.INSTANCE)
        httpResponseBody = new String(httpReply.array())
        then:
        httpResponseBody.contains('404')

        when:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist(netWorkers, slotNumber)
        localPersist.fixSlotThreadId(slot0, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId(slot1, Thread.currentThread().threadId())
        def snowFlake = new SnowFlake(1, 1)
        m.requestHandlerArray[0] = new RequestHandler(workerId0, netWorkers, slotNumber, snowFlake, config)
        m.requestHandlerArray[1] = new RequestHandler(workerId1, netWorkers, slotNumber, snowFlake, config)
        m.scheduleRunnableArray[0] = new TaskRunnable(workerId0, netWorkers)
        m.scheduleRunnableArray[1] = new TaskRunnable(workerId1, netWorkers)
        then:
        1 == 1

        when:
        m.configInject = config
        def eventloop0 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop0.keepAlive(true)
        def eventloop1 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop1.keepAlive(true)
        Thread.start {
            eventloop0.run()
        }
        Thread.start {
            eventloop1.run()
        }
        Thread.sleep(100)
        m.netWorkerEventloopArray[0] = eventloop0
        m.netWorkerEventloopArray[1] = eventloop1
        m.onStart()
        then:
        1 == 1

        when:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def socket = TcpSocket.wrapChannel(eventloop0, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)
        // handle request
        def getData2 = new byte[2][]
        getData2[0] = 'get'.bytes
        getData2[1] = 'key'.bytes
        def getRequest = new Request(getData2, false, false)
        getRequest.slotNumber = slotNumber
        RequestHandler.parseSlots(getRequest)
        def p = m.handleRequest(getRequest, socket)
        eventloopCurrent.run()
        then:
        p.whenResult { reply ->
            reply != null
        }.result

        when:
        def data3 = new byte[3][]
        data3[0] = 'copy'.bytes
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        // for async reply
        def copyRequest = new Request(data3, false, false)
        copyRequest.slotNumber = slotNumber
        RequestHandler.parseSlots(copyRequest)
        p = m.handleRequest(copyRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        def mgetData6 = new byte[6][]
        mgetData6[0] = 'mget'.bytes
        mgetData6[1] = 'key1'.bytes
        mgetData6[2] = 'key2'.bytes
        mgetData6[3] = 'key3'.bytes
        mgetData6[4] = 'key4'.bytes
        mgetData6[5] = 'key5'.bytes
        def mgetRequest = new Request(mgetData6, true, false)
        mgetRequest.slotNumber = slotNumber
        RequestHandler.parseSlots(mgetRequest)
        p = m.handleRequest(mgetRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        // ping need not parse slots, any net worker can handle it
        def pingData = new byte[1][]
        pingData[0] = 'ping'.bytes
        def pingRequest = new Request(pingData, false, false)
        pingRequest.slotNumber = slotNumber
        RequestHandler.parseSlots(pingRequest)
        p = m.handleRequest(pingRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        pingRequest = new Request(pingData, true, false)
        p = m.handleRequest(pingRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        // no slots and cross request worker
        def flushdbData = new byte[1][]
        flushdbData[0] = 'flushdb'.bytes
        def flushdbRequest = new Request(flushdbData, false, false)
        flushdbRequest.slotNumber = slotNumber
        RequestHandler.parseSlots(flushdbRequest)
        p = m.handleRequest(flushdbRequest, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        def flushdbRequest2 = new Request(flushdbData, true, false)
        p = m.handleRequest(flushdbRequest2, socket)
        eventloopCurrent.run()
        then:
        p != null

        when:
        p = m.handlePipeline(null, socket, slotNumber)
        then:
        p != null

        when:
        ArrayList<Request> pipeline = new ArrayList<>()
        pipeline << getRequest
        p = m.handlePipeline(pipeline, socket, slotNumber)
        eventloopCurrent.run()
        then:
        p != null

        when:
        pipeline << getRequest
        p = m.handlePipeline(pipeline, socket, slotNumber)
        eventloopCurrent.run()
        then:
        p != null

        when:
        m.onStop()
        then:
        m.requestHandlerArray[0].isStopped
        m.requestHandlerArray[1].isStopped

        cleanup:
        eventloop0.breakEventloop()
        eventloop1.breakEventloop()
    }
}
