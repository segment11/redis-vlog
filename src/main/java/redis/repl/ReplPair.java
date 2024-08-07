package redis.repl;

import io.activej.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.RequestHandler;
import redis.repl.content.Hello;

import java.util.LinkedList;

public class ReplPair {
    public ReplPair(byte slot, boolean asMaster, String host, int port) {
        this.slot = slot;
        this.asMaster = asMaster;
        this.host = host;
        this.port = port;
    }

    private final byte slot;
    private final boolean asMaster;
    private final String host;
    private final int port;

    private final Logger log = LoggerFactory.getLogger(ReplPair.class);

    public byte getSlot() {
        return slot;
    }

    public String getHostAndPort() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        var replPair = (ReplPair) obj;
        return asMaster == replPair.asMaster && port == replPair.port && host.equals(replPair.host);
    }

    public boolean isAsMaster() {
        return asMaster;
    }

    public long getMasterUuid() {
        return masterUuid;
    }

    public void setMasterUuid(long masterUuid) {
        this.masterUuid = masterUuid;
    }

    public long getSlaveUuid() {
        return slaveUuid;
    }

    public void setSlaveUuid(long slaveUuid) {
        this.slaveUuid = slaveUuid;
    }

    public long getLastPingGetTimestamp() {
        return lastPingGetTimestamp;
    }

    public void setLastPingGetTimestamp(long lastPingGetTimestamp) {
        this.lastPingGetTimestamp = lastPingGetTimestamp;
    }

    public long getLastPongGetTimestamp() {
        return lastPongGetTimestamp;
    }

    public void setLastPongGetTimestamp(long lastPongGetTimestamp) {
        this.lastPongGetTimestamp = lastPongGetTimestamp;
    }

    private long masterUuid;
    private long slaveUuid;

    // client side send ping, server side update timestamp
    private long lastPingGetTimestamp;
    // server side send pong, client side update timestamp
    private long lastPongGetTimestamp;

    public boolean isLinkUp() {
        if (asMaster) {
            var isPingReceivedOk = System.currentTimeMillis() - lastPingGetTimestamp < 1000 * 3;
            return isPingReceivedOk;
        } else {
            var isPongReceivedOk = System.currentTimeMillis() - lastPongGetTimestamp < 1000 * 3
                    && tcpClient != null && tcpClient.isSocketConnected();
            return isPongReceivedOk;
        }
    }

    // only for slave pull, master never push
    private TcpClient tcpClient;

    public void initAsSlave(Eventloop eventloop, RequestHandler requestHandler) {
        // for unit test
        if (eventloop == null) {
            return;
        }

        if (System.currentTimeMillis() - lastPongGetTimestamp < 1000 * 3
                && tcpClient != null && tcpClient.isSocketConnected()) {
            log.warn("Repl pair init as slave: already connected, target host: {}, port: {}, slot: {}", host, port, slot);
        } else {
//            if (tcpClient != null) {
//                tcpClient.close();
//                log.warn("Repl pair init as slave: close old connection, target host: {}, port: {}, slot: {}", host, port, slot);
//            }

            var replContent = new Hello(slaveUuid, ConfForSlot.global.netListenAddresses);

            tcpClient = new TcpClient(slot, eventloop, requestHandler, this);
            tcpClient.connect(host, port, () -> Repl.buffer(slaveUuid, slot, ReplType.hello, replContent));
        }
    }

    boolean isSendBye = false;

    public boolean isSendBye() {
        return isSendBye;
    }

    public void setSendByeForTest(boolean isSendBye) {
        this.isSendBye = isSendBye;
    }

    public boolean bye() {
        if (tcpClient != null) {
            isSendBye = true;
            return tcpClient.bye();
        }
        return false;
    }

    public boolean ping() {
        if (isSendBye) {
            return false;
        }

        if (tcpClient != null) {
            return tcpClient.ping();
        }
        return false;
    }

    public boolean write(ReplType type, ReplContent content) {
        if (isSendBye) {
            return false;
        }

        if (tcpClient != null) {
            return tcpClient.write(type, content);
        }
        return false;
    }

    public void close() {
        if (tcpClient != null) {
            tcpClient.close();
            tcpClient = null;
        }
    }

    // as slave delay pull incremental big string file from master when catch up
    private LinkedList<Long> toFetchBigStringUuidList = new LinkedList<>();
    private LinkedList<Long> doFetchingBigStringUuidList = new LinkedList<>();

    public LinkedList<Long> getToFetchBigStringUuidList() {
        return toFetchBigStringUuidList;
    }

    public LinkedList<Long> getDoFetchingBigStringUuidList() {
        return doFetchingBigStringUuidList;
    }

    public void addToFetchBigStringUuid(long uuid) {
        toFetchBigStringUuidList.add(uuid);
    }

    public long doingFetchBigStringUuid() {
        if (toFetchBigStringUuidList.isEmpty()) {
            return -1;
        }
        var first = toFetchBigStringUuidList.pollFirst();
        doFetchingBigStringUuidList.add(first);
        return first;
    }

    public void doneFetchBigStringUuid(long uuid) {
        doFetchingBigStringUuidList.removeIf(e -> e == uuid);
    }
}
