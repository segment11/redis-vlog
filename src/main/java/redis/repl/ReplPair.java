package redis.repl;

import io.activej.eventloop.Eventloop;
import redis.ConfForSlot;
import redis.RequestHandler;
import redis.repl.content.Hello;

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

    public byte getSlot() {
        return slot;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ReplPair replPair = (ReplPair) obj;
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
            var isPingOk = System.currentTimeMillis() - lastPingGetTimestamp < 1000 * 3
                    && tcpClient != null && tcpClient.isSocketConnected();
            return isPingOk;
        } else {
            var isPongOk = System.currentTimeMillis() - lastPongGetTimestamp < 1000 * 3
                    && tcpClient != null && tcpClient.isSocketConnected();
            return isPongOk;
        }
    }

    private TcpClient tcpClient;

    public void initAsSlave(Eventloop eventloop, RequestHandler requestHandler) {
        if (System.currentTimeMillis() - lastPongGetTimestamp < 1000 * 3
                && tcpClient != null && tcpClient.isSocketConnected()) {
        } else {
            if (tcpClient != null) {
                tcpClient.close();
            }

            tcpClient = new TcpClient(slot, eventloop, requestHandler, this);

            var replContent = new Hello(slaveUuid, ConfForSlot.global.netListenAddresses);
            tcpClient.connect(host, port, () -> Repl.buffer(slaveUuid, slot, ReplType.hello, replContent));
        }
    }

    public void initAsMaster(long slaveUuid, Eventloop eventloop, RequestHandler requestHandler) {
        if (System.currentTimeMillis() - lastPingGetTimestamp < 1000 * 3 && slaveUuid == this.slaveUuid
                && tcpClient != null && tcpClient.isSocketConnected()) {
        } else {
            if (tcpClient != null) {
                tcpClient.close();
            }

            this.slaveUuid = slaveUuid;

            tcpClient = new TcpClient(slot, eventloop, requestHandler, this);
            tcpClient.connect(host, port, null);
        }
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

    private boolean isSendBye = false;

    public boolean bye() {
        if (tcpClient != null) {
            isSendBye = true;
            return tcpClient.bye();
        }
        return false;
    }

    public void close() {
        if (tcpClient != null) {
            tcpClient.close();
        }
    }
}
