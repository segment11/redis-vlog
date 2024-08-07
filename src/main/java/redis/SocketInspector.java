package redis;

import io.activej.bytebuf.ByteBuf;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.prometheus.client.Gauge;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class SocketInspector implements TcpSocket.Inspector {
    private final Logger log = LoggerFactory.getLogger(SocketInspector.class);

    final ConcurrentHashMap<InetSocketAddress, TcpSocket> socketMap = new ConcurrentHashMap<>();
    private final HashMap<ITcpSocket, Byte> clientDBSelected = new HashMap<>();

    public Byte getDBSelected(ITcpSocket socket) {
        return clientDBSelected.get(socket);
    }

    public void setDBSelected(ITcpSocket socket, byte db) {
        clientDBSelected.put(socket, db);
    }

    static final String EXTEND_KEY_PREFIX = "_/";

    public String addExtendKeyPrefixByDBSelected(ITcpSocket socket, String rawKey) {
        var db = getDBSelected(socket);
        if (db == null) {
            return rawKey;
        }
        if (db == 0) {
            return rawKey;
        }
        return EXTEND_KEY_PREFIX + db + "_" + rawKey;
    }

    private int maxConnections = 1000;

    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    // inject, singleton, need not static
    static final Gauge connectedCountGauge = Gauge.build()
            .name("connected_client_count")
            .help("connected client count")
            .register();

    @Override
    public void onConnect(TcpSocket socket) {
        if (socketMap.size() >= maxConnections) {
            log.warn("Max connections reached: {}, close the socket", maxConnections);
            socket.close();
            return;
        }

        var remoteAddress = socket.getRemoteAddress();
        log.info("On connect, remote address: {}", remoteAddress);
        socketMap.put(remoteAddress, socket);

        connectedCountGauge.inc();
    }

    @Override
    public void onReadTimeout(TcpSocket socket) {

    }

    @Override
    public void onRead(TcpSocket socket, ByteBuf buf) {

    }

    @Override
    public void onReadEndOfStream(TcpSocket socket) {

    }

    @Override
    public void onReadError(TcpSocket socket, IOException e) {

    }

    @Override
    public void onWriteTimeout(TcpSocket socket) {

    }

    @Override
    public void onWrite(TcpSocket socket, ByteBuf buf, int bytes) {

    }

    @Override
    public void onWriteError(TcpSocket socket, IOException e) {

    }

    @Override
    public void onDisconnect(TcpSocket socket) {
        var remoteAddress = socket.getRemoteAddress();
        log.info("On disconnect, remote address: {}", remoteAddress);
        AfterAuthFlagHolder.remove(remoteAddress);
        socketMap.remove(remoteAddress);
        clientDBSelected.remove(socket);

        connectedCountGauge.dec();
    }

    @Override
    public <T extends TcpSocket.Inspector> @Nullable T lookup(Class<T> type) {
        return null;
    }

    public void clearAll() {
        socketMap.clear();
    }
}
