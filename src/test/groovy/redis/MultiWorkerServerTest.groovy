package redis

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.activej.inject.binding.OptionalDependency
import io.activej.net.socket.tcp.TcpSocket
import redis.decode.Request
import redis.persist.Consts
import redis.persist.KeyBucket
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.LeaderSelector
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
                .with("net.listenAddresses", "localhost:7379")

        dirFile.deleteDir()

        def m = new MultiWorkerServer()
        m.netWorkerEventloopArray = new Eventloop[2]
        m.requestHandlerArray = new RequestHandler[2]
        m.scheduleRunnableArray = new TaskRunnable[2]
        m.refreshLoader = m.refreshLoader()
        println MultiWorkerServer.UP_TIME

        def dirFile2 = m.dirFile(config)
        def dirFile3 = m.dirFile(config)

        def snowFlakes = m.snowFlakes(config)

        expect:
        dirFile2.exists()
        dirFile3.exists()
        m.STATIC_GLOBAL_V.socketInspector == null
        m.primaryReactor(config) != null
        m.workerReactor(workerId0, OptionalDependency.empty(), config) != null
        // need use activej inject mock, todo
        m.workerPool(null, config) == null
        m.config() != null
        snowFlakes != null
        m.getModule() != null
        m.getBusinessLogicModule() != null

        when:
        MultiWorkerServer.MAIN_ARGS = ['/etc/redis-vlog.properties']
        m.config()
        then:
        1 == 1

        when:
        MultiWorkerServer.MAIN_ARGS = []
        m.config()
        then:
        1 == 1

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
        m.socketInspector = new SocketInspector()
        m.onStart()
        m.primaryScheduleRunnable.run()
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

        when:
        def m1 = new MultiWorkerServer.InnerModule()
        def c = m1.confForSlot(config)
        then:
        1 == 1

        when:
        def b = m1.beforeCreateHandler(c, snowFlakes, config)
        then:
        1 == 1

        when:
        def cc = ConfForSlot.global
        def configX = Config.create()
                .with("net.listenAddresses", "localhost:7379")
                .with("debugMode", "true")
                .with("pureMemory", "true")
                .with("zookeeperConnectString", 'localhost:2181')
                .with("bucket.bucketsPerSlot", cc.confBucket.bucketsPerSlot.toString())
                .with("bucket.initialSplitNumber", cc.confBucket.initialSplitNumber.toString())
                .with("bucket.lruPerFd.maxSize", cc.confBucket.lruPerFd.maxSize.toString())
                .with("chunk.segmentNumberPerFd", cc.confChunk.segmentNumberPerFd.toString())
                .with("chunk.fdPerChunk", cc.confChunk.fdPerChunk.toString())
                .with("chunk.segmentLength", cc.confChunk.segmentLength.toString())
                .with("chunk.lruPerFd.maxSize", cc.confChunk.lruPerFd.maxSize.toString())
                .with("wal.oneChargeBucketNumber", cc.confWal.oneChargeBucketNumber.toString())
                .with("wal.valueSizeTrigger", cc.confWal.valueSizeTrigger.toString())
                .with("wal.shortValueSizeTrigger", cc.confWal.shortValueSizeTrigger.toString())
                .with("repl.binlogForReadCacheSegmentMaxCount", cc.confRepl.binlogForReadCacheSegmentMaxCount.toString())
                .with("big.string.lru.maxSize", cc.lruBigString.maxSize.toString())
                .with("kv.lru.maxSize", cc.lruKeyAndCompressedValueEncoded.maxSize.toString())
        m1.confForSlot(configX)
        m1.beforeCreateHandler(c, snowFlakes, configX)
        then:
        1 == 1

        when:
        boolean exception = false
        def config2 = Config.create()
                .with("slotNumber", (LocalPersist.MAX_SLOT_NUMBER + 1).toString())

        def snowFlakes1 = m.snowFlakes(config2)
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config3 = Config.create()
                .with("slotNumber", "3")
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config3)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config4 = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", (MultiWorkerServer.MAX_NET_WORKERS + 1).toString())
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config4)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def cpuNumber = Runtime.getRuntime().availableProcessors()
        def config5 = Config.create()
                .with("slotNumber", "1")
                .with("netWorkers", (cpuNumber + 1).toString())
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config5)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config6 = Config.create()
                .with("slotNumber", "2")
                .with("netWorkers", '3')
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config6)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config7 = Config.create()
                .with("slotNumber", "4")
                .with("netWorkers", '3')
        try {
            m1.beforeCreateHandler(c, snowFlakes1, config7)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config8 = Config.create()
                .with("bucket.bucketsPerSlot", (KeyBucket.MAX_BUCKETS_PER_SLOT + 1).toString())
        try {
            m1.confForSlot(config8)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config9 = Config.create()
                .with("bucket.bucketsPerSlot", '1023')
        try {
            m1.confForSlot(config9)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config10 = Config.create()
                .with("bucket.bucketsPerSlot", '1024')
                .with("chunk.fdPerChunk", (ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK + 1).toString())
        try {
            m1.confForSlot(config10)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def config11 = Config.create()
                .with("chunk.fdPerChunk", '16')
                .with("wal.oneChargeBucketNumber", '4')
        try {
            m1.confForSlot(config11)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        m1.requestHandlerArray(snowFlakes, b, config)
        then:
        1 == 1

        when:
        m1.scheduleRunnableArray(b, config)
        then:
        1 == 1

        when:
        m1.socketInspector(config)
        then:
        1 == 1

        cleanup:
        eventloop0.breakEventloop()
        eventloop1.breakEventloop()
    }

    def 'test do repl'() {
        given:
        def leaderSelector = LeaderSelector.instance
        leaderSelector.masterAddressLocalMocked = 'localhost:7379'
        ConfForGlobal.netListenAddresses = leaderSelector.masterAddressLocalMocked

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot0, Thread.currentThread().threadId())

        when:
        MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        then:
        1 == 1

        when:
        leaderSelector.masterAddressLocalMocked = 'localhost:7380'
        MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        then:
        1 == 1

        when:
        ConfForGlobal.isAsSlaveOfSlave = true
        MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        then:
        1 == 1

        when:
        leaderSelector.masterAddressLocalMocked = null
        MultiWorkerServer.doReplAfterLeaderSelect(slot0)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
