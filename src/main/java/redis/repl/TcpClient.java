package redis.repl;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.TcpSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.RequestHandler;
import redis.command.XGroup;
import redis.decode.RequestDecoder;
import redis.repl.content.Ping;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

public class TcpClient {
    private final byte slot;
    private final Eventloop eventloop;
    private final RequestHandler requestHandler;
    private final ReplPair replPair;

    public TcpClient(byte slot, Eventloop eventloop, RequestHandler requestHandler, ReplPair replPair) {
        this.slot = slot;
        this.eventloop = eventloop;
        this.requestHandler = requestHandler;
        this.replPair = replPair;
    }

    private final Logger log = LoggerFactory.getLogger(TcpClient.class);

    private TcpSocket sock;

    boolean isSocketConnected() {
        return sock != null && !sock.isClosed();
    }

    public boolean write(ReplType type, ReplContent content) {
        if (isSocketConnected()) {
            try {
                sock.write(Repl.buffer(replPair.getSlaveUuid(), slot, type, content));
                return true;
            } catch (Exception e) {
                log.error("Could not write to server", e);
                return false;
            }
        } else {
            log.error("Socket is not connected");
            return false;
        }
    }

    public boolean ping() {
        return write(ReplType.ping, new Ping(ConfForSlot.global.netListenAddresses));
    }

    public void connect(String host, int port, Callable<ByteBuf> callback) {
        TcpSocket.connect(eventloop, new InetSocketAddress(host, port))
                .whenResult(socket -> {
                    log.info("Connected to server at {}:{}, slot: {}", host, port, slot);
                    sock = socket;

                    BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                            .decodeStream(new RequestDecoder())
                            .map(request -> {
                                if (request == null) {
                                    return null;
                                }

                                var xGroup = new XGroup(null, request.getData(), socket);
                                xGroup.init(requestHandler, request);
                                xGroup.setReplPair(replPair);
                                var reply = xGroup.handleRepl();
                                if (reply == null) {
                                    return null;
                                }
                                return reply.buffer();
                            })
                            .streamTo(ChannelConsumers.ofSocket(socket));

                    if (callback != null) {
                        sock.write(callback.call());
                    }
                })
                .whenException(e -> log.error("Could not connect to server", e));
    }

    public void close() {
        if (sock != null && !sock.isClosed()) {
            sock.close();
        }
    }
}
