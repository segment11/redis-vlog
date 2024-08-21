package redis.repl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForGlobal;
import redis.ConfForSlot;
import redis.command.XGroup;
import redis.persist.LocalPersist;
import redis.repl.support.JedisPoolHolder;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class LeaderSelector {
    // singleton
    private LeaderSelector() {
    }

    private static final LeaderSelector instance = new LeaderSelector();

    public static LeaderSelector getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private CuratorFramework client;

    synchronized void connect() {
        var connectString = ConfForGlobal.zookeeperConnectString;
        if (connectString == null) {
            log.debug("Repl zookeeper connect string is null, leader select will not work");
            return;
        }

        if (client != null) {
            log.warn("Repl zookeeper client already started, connect string: {}", connectString);
            return;
        }

        client = CuratorFrameworkFactory.newClient(connectString,
                new ExponentialBackoffRetry(1000, 3));
        client.start();
        log.info("Repl zookeeper client started, connect string: {}", connectString);
    }

    private boolean isConnected() {
        return client != null && client.getZookeeperClient().isConnected();
    }

    private long isLeaderLoopCount = 0;

    public String tryConnectAndGetMasterListenAddress() {
        if (!isConnected()) {
            connect();
            return null;
        }

        if (!ConfForGlobal.canBeLeader) {
            return getMasterListenAddressAsSlave();
        }

        var isStartOk = startLeaderLatch();
        if (!isStartOk) {
            return null;
        }

        if (leaderLatch.hasLeadership()) {
            var path = ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LISTEN_ADDRESS_PATH;

            // set listen address to zookeeper leader listen address path
            var listenAddress = ConfForGlobal.netListenAddresses;
            try {
                client.create().withMode(CreateMode.EPHEMERAL).forPath(path, listenAddress.getBytes());

                if (isLeaderLoopCount % 10 == 0) {
                    log.info("Repl set master listen address to zookeeper: {}", listenAddress);
                }
            } catch (Exception e) {
                // need not stack trace
                log.error("Repl set master listen address to zookeeper failed: " + e.getMessage());
                return null;
            }

            isLeaderLoopCount++;
            return listenAddress;
        } else {
            return getMasterListenAddressAsSlave();
        }
    }

    void removeTargetPathForTest() throws Exception {
        var path = ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LISTEN_ADDRESS_PATH;
        client.delete().forPath(path);
        log.info("Repl remove master listen address from zookeeper: {}", path);
    }

    @Nullable
    private String getMasterListenAddressAsSlave() {
        var path = ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LISTEN_ADDRESS_PATH;

        try {
            var data = client.getData().forPath(path);
//            if (data == null) {
//                log.warn("Repl get master listen address from zookeeper failed, data is null");
//                return null;
//            }

            var listenAddress = new String(data);
            log.info("Repl get master listen address from zookeeper: {}", listenAddress);
            return listenAddress;
        } catch (Exception e) {
            // need not stack trace
            log.error("Repl get master listen address from zookeeper failed: " + e.getMessage());
            return null;
        }
    }

    private synchronized void close() {
        if (client != null) {
            client.close();
            log.info("Repl zookeeper client closed");
        }
    }

    private LeaderLatch leaderLatch;

    synchronized boolean startLeaderLatch() {
        if (leaderLatch != null) {
            log.debug("Repl leader latch already started");
            return true;
        }

        // client must not be null
        leaderLatch = new LeaderLatch(client, ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LATCH_PATH);
        try {
            leaderLatch.start();
            log.info("Repl leader latch started");
            leaderLatch.await(10, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            // need not stack trace
            log.error("Repl leader latch start failed: " + e.getMessage());
            return false;
        }
    }

    public synchronized void closeLeaderLatch() {
        if (leaderLatch != null) {
            try {
                leaderLatch.close();
                log.info("Repl leader latch closed");
            } catch (Exception e) {
                // need not stack trace
                log.error("Repl leader latch close failed: " + e.getMessage());
            }
        }
    }

    public void closeAll() {
        closeLeaderLatch();
        close();
    }

    public void resetAsMaster(boolean returnExceptionIfAlreadyIsMaster, Consumer<Exception> callback) {
        var localPersist = LocalPersist.getInstance();

        // when support cluster, need to check all slots, todo
        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        if (!firstOneSlot.isAsSlave()) {
            // already is master
            if (returnExceptionIfAlreadyIsMaster) {
                callback.accept(new IllegalStateException("Repl already is master"));
            } else {
                callback.accept(null);
            }
            return;
        }

        Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
        for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((byte) i);
            promises[i] = oneSlot.asyncRun(() -> {
                // always true
                var isSelfSlave = oneSlot.removeReplPairAsSlave();

                if (isSelfSlave) {
                    // reset as master
                    oneSlot.persistMergingOrMergedSegmentsButNotPersisted();
                    oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(false);
                    oneSlot.getMergedSegmentIndexEndLastTime();
                }

                oneSlot.getBinlog().moveToNextSegment();
                oneSlot.resetReadonlyFalseAsMaster();
            });
        }

        Promises.all(promises).whenComplete((r, e) -> {
            callback.accept(e);
        });
    }

    public void resetAsSlave(boolean returnExceptionIfAlreadyIsSlave, String host, int port, Consumer<Exception> callback) {
        var localPersist = LocalPersist.getInstance();

        // when support cluster, need to check all slots, todo
        boolean needCloseOldReplPairAsSlave = false;
        var firstOneSlot = localPersist.currentThreadFirstOneSlot();
        if (firstOneSlot.isAsSlave()) {
            if (returnExceptionIfAlreadyIsSlave) {
                callback.accept(new IllegalStateException("Repl already is slave"));
                return;
            }

            // must not be null
            var replPair = firstOneSlot.getOnlyOneReplPairAsSlave();
            if (replPair.getHostAndPort().equals(host + ":" + port)) {
                // already is slave of target host and port
                log.debug("Repl already is slave of target host and port: {}:{}", host, port);
                callback.accept(null);
                return;
            } else {
                needCloseOldReplPairAsSlave = true;
            }
        }

        if (needCloseOldReplPairAsSlave) {
            Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
            for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
                var oneSlot = localPersist.oneSlot((byte) i);
                // still is a slave, need not reset readonly
                promises[i] = oneSlot.asyncRun(() -> {
                    oneSlot.removeReplPairAsSlave();
                    oneSlot.getBinlog().moveToNextSegment();
                });
            }

            Promises.all(promises).whenComplete((r, e) -> {
                if (e != null) {
                    callback.accept(e);
                    return;
                }

                makeSelfAsSlave(host, port, callback);
            });
        } else {
            makeSelfAsSlave(host, port, callback);
        }
    }

    // in primary eventloop
    private void makeSelfAsSlave(String host, int port, Consumer<Exception> callback) {
        try {
            var jedisPool = JedisPoolHolder.getInstance().create(host, port, null, 5000);
            // may be null
            var jsonStr = (String) JedisPoolHolder.exe(jedisPool, jedis -> {
                var pong = jedis.ping();
                log.info("Repl slave of {}:{} pong: {}", host, port, pong);
                return jedis.get(XGroup.CONF_FOR_SLOT_KEY);
            });

            var map = ConfForSlot.global.slaveCanMatchCheckValues();
            var objectMapper = new ObjectMapper();
            var jsonStrLocal = objectMapper.writeValueAsString(map);

            if (!jsonStrLocal.equals(jsonStr)) {
                callback.accept(new IllegalStateException("Repl slave can not match check values"));
            }
        } catch (Exception e) {
            callback.accept(e);
            return;
        }

        var localPersist = LocalPersist.getInstance();

        Promise<Void>[] promises = new Promise[ConfForGlobal.slotNumber];
        for (int i = 0; i < ConfForGlobal.slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((byte) i);
            promises[i] = oneSlot.asyncRun(() -> {
                oneSlot.createReplPairAsSlave(host, port);
                oneSlot.getBinlog().moveToNextSegment();
            });
        }

        Promises.all(promises).whenComplete((r, e) -> {
            callback.accept(e);
        });
    }

    public String getFirstSlaveListenAddressByMasterHostAndPort(String host, int port) {
        var jedisPool = JedisPoolHolder.getInstance().create(host, port, null, 5000);
        return (String) JedisPoolHolder.exe(jedisPool, jedis -> jedis.get(XGroup.GET_FIRST_SLAVE_LISTEN_ADDRESS_KEY));
    }
}
