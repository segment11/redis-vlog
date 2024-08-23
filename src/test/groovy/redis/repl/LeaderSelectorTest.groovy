package redis.repl

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.net.telnet.TelnetClient
import redis.ConfForGlobal
import redis.ConfForSlot
import redis.command.XGroup
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.support.JedisPoolHolder
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class LeaderSelectorTest extends Specification {
    def 'test base'() {
        given:
        def leaderSelector = LeaderSelector.instance
        // only for coverage
        leaderSelector.closeAll()

        expect:
        leaderSelector.tryConnectAndGetMasterListenAddress() == null

        when:
        def testListenAddress = 'localhost:7379'
        leaderSelector.masterAddressLocalForTest = testListenAddress
        then:
        leaderSelector.masterAddressLocalForTest == testListenAddress
        leaderSelector.tryConnectAndGetMasterListenAddress() == testListenAddress
        leaderSelector.getFirstSlaveListenAddressByMasterHostAndPort('localhost', 6379, slot) == testListenAddress
        leaderSelector.resetAsMaster(true, e -> { })
        leaderSelector.resetAsSlave(true, '', 0, e -> { })

        when:
        leaderSelector.masterAddressLocalForTest = null
        ConfForGlobal.zookeeperConnectString = 'localhost:2181'
        ConfForGlobal.zookeeperRootPath = '/redis-vlog/cluster-test'
        ConfForGlobal.netListenAddresses = testListenAddress

        boolean doThisCase = false
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect('localhost', 2181)
            doThisCase = true
        } catch (Exception ignored) {
        } finally {
            tc.disconnect()
        }
        if (!doThisCase) {
            ConfForGlobal.zookeeperConnectString = null
            println 'zookeeper not running, skip'
        }

        def masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        // already connected, skip, for coverage
        leaderSelector.connect()
        then:
        // just connect this time
        masterListenAddress == null

        when:
        Thread.sleep(1000)
        leaderSelector.startLeaderLatch()
        masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress()
        then:
        masterListenAddress == null || masterListenAddress == ConfForGlobal.netListenAddresses

        when:
        Thread.sleep(1000)
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
        String masterListenAddress2 = null
        if (doThisCase) {
            try {
                leaderSelector.removeTargetPathForTest()
                masterListenAddress2 = leaderSelector.tryConnectAndGetMasterListenAddress()
            } catch (Exception e) {
                println e.message
                masterListenAddress2 = null
            }
        }
        then:
        doThisCase ? (masterListenAddress2 == null) : (masterListenAddress2 == masterListenAddress)

        when:
        ConfForGlobal.canBeLeader = true
        leaderSelector.closeLeaderLatch()
        leaderSelector.closeLeaderLatch()
        Thread.sleep(1000)
        masterListenAddress = leaderSelector.tryConnectAndGetMasterListenAddress(false)
        then:
        masterListenAddress == null

        cleanup:
        leaderSelector.closeAll()
    }

    final byte slot = 0

    def 'test reset as master'() {
        given:
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
        JedisPoolHolder.instance.closeAll()
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
