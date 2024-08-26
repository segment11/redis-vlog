package redis.repl

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.net.telnet.TelnetClient
import redis.ConfForGlobal
import redis.ConfForSlot
import redis.SocketInspector
import redis.command.XGroup
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.support.JedisPoolHolder
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class LeaderSelectorTest extends Specification {
    static boolean checkZk() {
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect('localhost', 2181)
            return true
        } catch (Exception ignored) {
            return false
        } finally {
            tc.disconnect()
        }
    }

    def 'test leader latch'() {
        given:
        def leaderSelector = LeaderSelector.instance
        // only for coverage
        leaderSelector.cleanUp()

        expect:
        leaderSelector.tryConnectAndGetMasterListenAddress() == null

        when:
        def testListenAddress = 'localhost:7379'
        leaderSelector.masterAddressLocalMocked = testListenAddress
        then:
        !leaderSelector.isConnected()
        !leaderSelector.hasLeadership()
        leaderSelector.lastGetMasterListenAddressAsSlave == null
        leaderSelector.masterAddressLocalMocked == testListenAddress
        leaderSelector.tryConnectAndGetMasterListenAddress() == testListenAddress
        leaderSelector.getFirstSlaveListenAddressByMasterHostAndPort('localhost', 6379, slot) == testListenAddress
        leaderSelector.resetAsMaster(true, e -> { })
        leaderSelector.resetAsSlave(true, '', 0, e -> { })

        when:
        leaderSelector.hasLeadershipLocalMocked = false
        then:
        !leaderSelector.hasLeadership()

        when:
        leaderSelector.hasLeadershipLocalMocked = null
        leaderSelector.masterAddressLocalMocked = null
        ConfForGlobal.zookeeperConnectString = 'localhost:2181'
        ConfForGlobal.zookeeperRootPath = '/redis-vlog/cluster-test'
        ConfForGlobal.netListenAddresses = testListenAddress

        boolean doThisCase = checkZk()
        if (!doThisCase) {
            ConfForGlobal.zookeeperConnectString = null
            println 'zookeeper not running, skip'
        }

        def masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        // already connected, skip, for coverage
        leaderSelector.connect()
        then:
        masterListenAddress == (doThisCase ? ConfForGlobal.netListenAddresses : null)

        when:
        // already started, skip, for coverage
        leaderSelector.startLeaderLatch()
        // trigger log
        leaderSelector.isLeaderLoopCount = 99
        masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        then:
        masterListenAddress == (doThisCase ? ConfForGlobal.netListenAddresses : null)

        when:
        leaderSelector.startLeaderLatchFailMocked = true
        masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        then:
        masterListenAddress == null

        when:
        leaderSelector.startLeaderLatchFailMocked = false
        if (masterListenAddress == null) {
            masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        }
        masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        then:
        masterListenAddress == (doThisCase ? ConfForGlobal.netListenAddresses : null)

        when:
        ConfForGlobal.canBeLeader = false
        masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        then:
        masterListenAddress == (doThisCase ? ConfForGlobal.netListenAddresses : null)

        when:
        ConfForGlobal.canBeLeader = true
        if (doThisCase) {
            leaderSelector.stopLeaderLatch()
            // only for coverage
            leaderSelector.stopLeaderLatch()
            masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress(false)
        }
        then:
        masterListenAddress == null
        leaderSelector.lastStopLeaderLatchTimeMillis > (doThisCase ? 0 : -1)

        when:
        leaderSelector.disconnect()
        def isStartOk = leaderSelector.startLeaderLatch()
        then:
        !isStartOk
        !leaderSelector.isConnected()

        cleanup:
        leaderSelector.cleanUp()
    }

    final byte slot = 0

    def 'test reset as master'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:7380'
        LocalPersist.instance.socketInspector = new SocketInspector()

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def leaderSelector = LeaderSelector.instance

        when:
        CompletableFuture<Boolean> future = new CompletableFuture()
        leaderSelector.resetAsMaster(true) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        def r = future.get()
        then:
        // is already master, exception caught
        !r

        when:
        future = new CompletableFuture()
        leaderSelector.resetAsMaster(false) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        // is already master, skip
        r

        when:
        oneSlot.createReplPairAsSlave('localhost', 7379)
        future = new CompletableFuture()
        leaderSelector.resetAsMaster(false) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        !r

        when:
        var replPairAsSlave = oneSlot.onlyOneReplPairAsSlave
        replPairAsSlave.masterCanNotConnect = true
        future = new CompletableFuture()
        leaderSelector.resetAsMaster(false) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        r

        when:
        oneSlot.createReplPairAsSlave('localhost', 7379)
        replPairAsSlave = oneSlot.onlyOneReplPairAsSlave
        replPairAsSlave.masterCanNotConnect = false
        replPairAsSlave.masterReadonly = true
        replPairAsSlave.allCaughtUp = false
        future = new CompletableFuture()
        leaderSelector.resetAsMaster(false) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        !r

        when:
        replPairAsSlave.masterReadonly = false
        future = new CompletableFuture()
        leaderSelector.resetAsMaster(false) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        !r

        when:
        replPairAsSlave.masterReadonly = true
        replPairAsSlave.allCaughtUp = true
        future = new CompletableFuture()
        leaderSelector.resetAsMaster(false) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        r

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'reset as slave'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:7380'
        LocalPersist.instance.socketInspector = new SocketInspector()

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def leaderSelector = LeaderSelector.instance

        when:
        oneSlot.createReplPairAsSlave('localhost', 7379)

        CompletableFuture<Boolean> future = new CompletableFuture()
        leaderSelector.resetAsSlave(true, 'localhost', 7379) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        def r = future.get()
        then:
        // is already slave, exception caught
        !r

        when:
        future = new CompletableFuture()
        leaderSelector.resetAsSlave(false, 'localhost', 7379) { e ->
            if (e != null) {
                println e.message
                future.complete(false)
            } else {
                future.complete(true)
            }
        }
        r = future.get()
        then:
        // is already slave, target master is same, skip
        r

        // need redis-server running
        when:
        boolean doThisCase = false
        var map = ConfForSlot.global.slaveCanMatchCheckValues()
        var objectMapper = new ObjectMapper();
        var jsonStr = objectMapper.writeValueAsString(map)
        try {
            var jedisPool = JedisPoolHolder.instance.create('localhost', 6379)
            JedisPoolHolder.exe(jedisPool) { jedis ->
                jedis.set(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + "," +
                        XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD,
                        jsonStr + 'xxx')
                jedis.set(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + ",slot,0," +
                        XGroup.X_GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD,
                        'localhost:6380')
            }
            doThisCase = true
        } catch (Exception e) {
            println e.message
        }
        if (doThisCase) {
            // change master port, need close old as slave
            future = new CompletableFuture()
            leaderSelector.resetAsSlave(false, 'localhost', 6379) { e ->
                if (e != null) {
                    println e.message
                    future.complete(false)
                } else {
                    future.complete(true)
                }
            }
            r = future.get()
        } else {
            r = false
        }
        then:
        // json not match
        !r

        when:
        if (doThisCase) {
            var jedisPool = JedisPoolHolder.instance.create('localhost', 6379);
            JedisPoolHolder.exe(jedisPool) { jedis ->
                jedis.set(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + "," +
                        XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD,
                        jsonStr)
            }
            future = new CompletableFuture()
            leaderSelector.resetAsSlave(false, 'localhost', 6379) { e ->
                if (e != null) {
                    println e.message
                    future.complete(false)
                } else {
                    future.complete(true)
                }
            }
            r = future.get()
        } else {
            r = true
        }
        then:
        // json match
        r

        when:
        if (doThisCase) {
            oneSlot.removeReplPairAsSlave()
            future = new CompletableFuture()
            leaderSelector.resetAsSlave(false, 'localhost', 6379) { e ->
                if (e != null) {
                    println e.message
                    future.complete(false)
                } else {
                    future.complete(true)
                }
            }
            r = future.get()
        }
        then:
        // master become slave
        r

        when:
        def firstSlaveListenAddress = doThisCase ?
                leaderSelector.getFirstSlaveListenAddressByMasterHostAndPort('localhost', 6379, slot) :
                'localhost:6380'
        then:
        firstSlaveListenAddress == 'localhost:6380'

        cleanup:
        JedisPoolHolder.instance.cleanUp()
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
