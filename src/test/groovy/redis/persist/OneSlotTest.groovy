package redis.persist

import io.activej.common.function.RunnableEx
import io.activej.config.Config
import io.activej.eventloop.Eventloop
import redis.ConfVolumeDirsForSlot
import redis.RequestHandler
import redis.SnowFlake
import redis.repl.ReplPairTest
import spock.lang.Specification

import java.time.Duration

class OneSlotTest extends Specification {
    final byte slot = 0
    final short slotNumber = 1

    def 'test mock'() {
        given:
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)

        when:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        oneSlot.metaChunkSegmentIndex.cleanUp()

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def oneSlot2 = new OneSlot(slot, eventloopCurrent)
        oneSlot2.threadIdProtectedForSafe = Thread.currentThread().threadId()
        def call = oneSlot2.asyncCall(() -> 1)
        def run = oneSlot2.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        call.result
        run != null

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        def oneSlot3 = new OneSlot(slot, eventloop)
        Thread.start {
            eventloop.run()
        }
        call = oneSlot3.asyncCall(() -> 1)
        run = oneSlot3.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        call.whenResult { result -> result == 1 }.result
        run != null

        when:
        def oneSlot4 = new OneSlot(slot)
        then:
        oneSlot4.slot() == slot

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test init and repl pair'() {
        given:
        def persistConfig = Config.create()
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber)

        Consts.slotDir.deleteDir()

        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig)
        def oneSlot1 = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig)

        expect:
        oneSlot.slot() == slot
        oneSlot1.slot() == slot
        oneSlot.masterUuid > 0
        !oneSlot.isAsSlave()
        oneSlot.getReplPairAsSlave(11L) == null

        when:
        def persistConfig2 = Config.create().with('volumeDirsBySlot',
                '/tmp/data0:0-31,/tmp/data1:32-63,/tmp/data2:64-95,/tmp/data3:96-127')
        def tmpTestSlotNumber = (short) 128
        ConfVolumeDirsForSlot.initFromConfig(persistConfig2, tmpTestSlotNumber)
        def oneSlot0 = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig2)
        def oneSlot32 = new OneSlot((byte) 32, tmpTestSlotNumber, snowFlake, Consts.persistDir, persistConfig2)
        def oneSlot32_ = new OneSlot((byte) 32, tmpTestSlotNumber, snowFlake, Consts.persistDir, persistConfig2)
        then:
        oneSlot0.slot() == slot
        oneSlot32.slot() == (byte) 32
        oneSlot32_.slot() == (byte) 32
        oneSlot0.slotDir.absolutePath == '/tmp/data0/slot-0'
        oneSlot32.slotDir.absolutePath == '/tmp/data1/slot-32'

        when:
        // test repl pair
        def replPairAsMaster0 = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster0.slaveUuid = 11L
        def replPairAsMaster1 = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster1.slaveUuid = 12L
        // add 12L first
        oneSlot.replPairs.add(replPairAsMaster1)
        oneSlot.replPairs.add(replPairAsMaster0)
        def replPairAsSlave0 = oneSlot.createReplPairAsSlave('localhost', 6379)
        def replPairAsSlave1 = oneSlot.createReplPairAsSlave('localhost', 6379)
        replPairAsSlave0.sendByeForTest = true
        then:
        oneSlot.replPairs.size() == 4
        oneSlot.delayNeedCloseReplPairs.size() == 0
        oneSlot.getReplPairAsMaster(11L) != null
        oneSlot.getReplPairAsSlave(11L) == null
        oneSlot.isAsSlave()

        when:
        replPairAsSlave0.sendByeForTest = true
        replPairAsSlave1.sendByeForTest = false
        oneSlot.removeReplPairAsSlave()
        then:
        oneSlot.delayNeedCloseReplPairs.size() == 1

        when:
        // clear all
        oneSlot.replPairs.clear()
        oneSlot.delayNeedCloseReplPairs.clear()
        // add 2 as slaves
        oneSlot.replPairs.add(replPairAsSlave0)
        oneSlot.replPairs.add(replPairAsSlave1)
        replPairAsSlave0.sendByeForTest = false
        replPairAsSlave1.sendByeForTest = false
        oneSlot.removeReplPairAsSlave()
        then:
        oneSlot.replPairs.size() == 2
        oneSlot.delayNeedCloseReplPairs.size() == 2

        when:
        // clear all
        oneSlot.replPairs.clear()
        oneSlot.delayNeedCloseReplPairs.clear()
        then:
        oneSlot.getReplPairAsMaster(11L) == null

        when:
        oneSlot.replPairs.add(replPairAsMaster0)
        replPairAsMaster0.sendByeForTest = true
        then:
        oneSlot.getReplPairAsMaster(11L) == null

        when:
        replPairAsMaster0.sendByeForTest = false
        then:
        oneSlot.getReplPairAsMaster(11L) != null

        when:
        oneSlot.replPairs.clear()
        oneSlot.replPairs.add(replPairAsMaster1)
        then:
        oneSlot.getReplPairAsMaster(11L) == null

        when:
        oneSlot.replPairs.clear()
        oneSlot.replPairs.add(replPairAsSlave0)
        then:
        oneSlot.getReplPairAsMaster(11L) == null
        oneSlot.getReplPairAsSlave(oneSlot.masterUuid) != null

        when:
        oneSlot.replPairs.clear()
        oneSlot.replPairs.add(replPairAsSlave0)
        oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        then:
        oneSlot.replPairs.size() == 2

        when:
        // already exist one as master
        oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        then:
        oneSlot.replPairs.size() == 2

        cleanup:
        Consts.persistDir.deleteDir()
    }

    def 'test eventloop'() {
        given:
        def persistConfig = Config.create()
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber)

        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig)

        and:
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, slotNumber, null, null, Config.create())
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        oneSlot.netWorkerEventloop = eventloop
        oneSlot.requestHandler = requestHandler
        Thread.sleep(100)
        oneSlot.threadIdProtectedForSafe = eventloop.eventloopThread.threadId()

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        when:
        def p = oneSlot.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        p.whenComplete(RunnableEx.of(() -> {
            println 'complete async run'
            true
        })).result

        when:
        def p2 = oneSlot.asyncCall {
            println 'async call'
            1
        }
        eventloopCurrent.run()
        then:
        p2.whenComplete((i, e) -> {
            println 'complete async call'
            i == 1
        }).result

        when:
        int[] array = [0]
        oneSlot.delayRun(100, () -> {
            println 'delay run'
            array[0] = 1
        })
        eventloopCurrent.run()
        Thread.sleep(100)
        then:
        array[0] == 1

        when:
        oneSlot.netWorkerEventloop = eventloopCurrent
        oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId()
        def p11 = oneSlot.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        p11 != null

        when:
        def p22 = oneSlot.asyncCall {
            println 'async call'
            1
        }
        eventloopCurrent.run()
        then:
        p22.whenComplete((i, e) -> {
            println 'complete async call'
            i == 1
        }).result

        cleanup:
        eventloop.breakEventloop()
    }
}
