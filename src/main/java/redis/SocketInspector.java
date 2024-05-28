package redis;

import io.activej.bytebuf.ByteBuf;
import io.activej.net.socket.tcp.TcpSocket;
import io.prometheus.client.Gauge;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class SocketInspector implements TcpSocket.Inspector {
    private final Logger log = LoggerFactory.getLogger(SocketInspector.class);

    ConcurrentHashMap<InetSocketAddress, TcpSocket> socketMap = new ConcurrentHashMap<>();

    // inject, singleton, need not static
    private final Gauge connectedCountGauge = Gauge.build()
            .name("connected_client_count")
            .help("connected client count")
            .register();

    @Override
    public void onConnect(TcpSocket socket) {
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
        AuthHolder.flagBySocketAddress.remove(remoteAddress);
        socketMap.remove(remoteAddress);

        connectedCountGauge.dec();
    }

    @Override
    public <T extends TcpSocket.Inspector> @Nullable T lookup(Class<T> type) {
        return null;
    }

    public void closeAll() {
        try {
            for (var socket : socketMap.values()) {
                if (!socket.isClosed()) {
                    socket.close();
                }
            }
        } catch (Exception e) {
            System.out.println("Error in close all sockets: " + e.getMessage());
        }
        System.out.println("All sockets closed");
    }

    public void clearAll() {
        socketMap.clear();
    }
}
