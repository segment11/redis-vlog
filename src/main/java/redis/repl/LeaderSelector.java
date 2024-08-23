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
import redis.ForTestMethod;
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

    @ForTestMethod
    public String getMasterAddressLocalForTest() {
        return masterAddressLocalForTest;
    }

    @ForTestMethod
    public void setMasterAddressLocalForTest(String masterAddressLocalForTest) {
        this.masterAddressLocalForTest = masterAddressLocalForTest;
    }

    @ForTestMethod
    private String masterAddressLocalForTest;

    public String tryConnectAndGetMasterListenAddress() {
        if (masterAddressLocalForTest != null) {
            return masterAddressLocalForTest;
        }

        return tryConnectAndGetMasterListenAddress(true);
    }

    public String tryConnectAndGetMasterListenAddress(boolean doStartLeaderLatch) {
        if (!isConnected()) {
            connect();
            return null;
        }

        if (!ConfForGlobal.canBeLeader) {
            return getMasterListenAddressAsSlave();
        }

        var isStartOk = !doStartLeaderLatch || startLeaderLatch();
        if (!isStartOk) {
            return null;
        }

        if (leaderLatch != null && leaderLatch.hasLeadership()) {
            var path = ConfForGlobal.zookeeperRootPath + ConfForGlobal.LEADER_LISTEN_ADDRESS_PATH;

            // set listen address to zookeeper leader listen address path
            var listenAddress = ConfForGlobal.netListenAddresses;
            try {
                var stat = client.checkExists().forPath(path);
                if (stat == null) {
                    client.create().withMode(CreateMode.EPHEMERAL).forPath(path, listenAddress.getBytes());
                } else {
                    // update
                    client.setData().forPath(path, listenAddress.getBytes());
                }

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

    @ForTestMethod
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
            log.debug("Repl get master listen address from zookeeper: {}", listenAddress);
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

        if (client == null) {
            log.error("Repl leader latch start failed: client is null");
            return false;
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
                leaderLatch = null;
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

    // run in primary eventloop
    public void resetAsMaster(boolean returnExceptionIfAlreadyIsMaster, Consumer<Exception> callback) {
        if (masterAddressLocalForTest != null) {
            callback.accept(null);
            callback.accept(new RuntimeException("just test callback"));
            return;
        }

        var localPersist = LocalPersist.getInstance();

        // when support cluster, need to check all slots, todo
        var firstOneSlot = localPersist.oneSlots()[0];
        var pp = firstOneSlot.asyncCall(firstOneSlot::isAsSlave);

        pp.whenComplete((isAsSlave, e) -> {
            if (e != null) {
                callback.accept(e);
                return;
            }

            if (!isAsSlave) {
                // already is master
                if (returnExceptionIfAlreadyIsMaster) {
                    callback.accept(new IllegalStateException("Repl already is master"));
                } else {
                    callback.accept(null);
                }
                return;
            }

            resetAsMasterNextStep(callback);
        });
    }

    private static void resetAsMasterNextStep(Consumer<Exception> callback) {
        var localPersist = LocalPersist.getInstance();

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

    // run in primary eventloop
    public void resetAsSlave(boolean returnExceptionIfAlreadyIsSlave, String host, int port, Consumer<Exception> callback) {
        if (masterAddressLocalForTest != null) {
            callback.accept(null);
            callback.accept(new RuntimeException("just test callback"));
            return;
        }

        var localPersist = LocalPersist.getInstance();

        // when support cluster, need to check all slots, todo
        var firstOneSlot = localPersist.oneSlots()[0];
        var pp = firstOneSlot.asyncCall(firstOneSlot::getOnlyOneReplPairAsSlave);

        pp.whenComplete((replPair, e) -> {
            if (e != null) {
                callback.accept(e);
                return;
            }

            boolean needCloseOldReplPairAsSlave = false;
            if (replPair != null) {
                if (returnExceptionIfAlreadyIsSlave) {
                    callback.accept(new IllegalStateException("Repl already is slave"));
                    return;
                }

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

                Promises.all(promises).whenComplete((r, ee) -> {
                    if (ee != null) {
                        callback.accept(ee);
                        return;
                    }

                    makeSelfAsSlave(host, port, callback);
                });
            } else {
                makeSelfAsSlave(host, port, callback);
            }
        });
    }

    // in primary eventloop
    private void makeSelfAsSlave(String host, int port, Consumer<Exception> callback) {
        try {
            var jedisPool = JedisPoolHolder.getInstance().create(host, port, null, 5000);
            // may be null
            var jsonStr = (String) JedisPoolHolder.exe(jedisPool, jedis -> {
                var pong = jedis.ping();
                log.info("Repl slave of {}:{} pong: {}", host, port, pong);
                return jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + "," + XGroup.CONF_FOR_SLOT_KEY);
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
                // do not write binlog as slave
                oneSlot.getDynConfig().setBinlogOn(false);
            });
        }

        Promises.all(promises).whenComplete((r, e) -> {
            callback.accept(e);
        });
    }

    public String getFirstSlaveListenAddressByMasterHostAndPort(String host, int port, byte slot) {
        if (masterAddressLocalForTest != null) {
            return masterAddressLocalForTest;
        }

        var jedisPool = JedisPoolHolder.getInstance().create(host, port, null, 5000);
        return (String) JedisPoolHolder.exe(jedisPool, jedis ->
                // refer to XGroup handle
                // key will be transferred to x_repl slot 0 get_first_slave_listen_address, refer to request handler
                jedis.get(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH + ",slot," + slot + "," +
                        XGroup.GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD)
        );
    }
}
