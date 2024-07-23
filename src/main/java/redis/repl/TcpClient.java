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
    private final Eventloop netWorkerEventloop;
    private final RequestHandler requestHandler;
    private final ReplPair replPair;

    public TcpClient(byte slot, Eventloop netWorkerEventloop, RequestHandler requestHandler, ReplPair replPair) {
        this.slot = slot;
        this.netWorkerEventloop = netWorkerEventloop;
        this.requestHandler = requestHandler;
        this.replPair = replPair;
    }

    private final Logger log = LoggerFactory.getLogger(TcpClient.class);

    private TcpSocket sock;

    boolean isSocketConnected() {
        return sock != null && !sock.isClosed();
    }

    long writeErrorCount = 0;
    long notConnectedErrorCount = 0;

    boolean write(ReplType type, ReplContent content) {
        if (isSocketConnected()) {
            try {
                sock.write(Repl.buffer(replPair.getSlaveUuid(), slot, type, content));
                writeErrorCount = 0;
                return true;
            } catch (Exception e) {
                // reduce log
                if (writeErrorCount % 1000 == 0) {
                    log.error("Could not write to server, to server: " +
                            replPair.getHostAndPort() + ", slot: " + slot, e);
                }
                writeErrorCount++;
                return false;
            } finally {
                notConnectedErrorCount = 0;
            }
        } else {
            if (notConnectedErrorCount % 1000 == 0) {
                log.error("Socket is not connected, to server: {}, slot: {}",
                        replPair.getHostAndPort(), slot);
            }
            notConnectedErrorCount++;
            return false;
        }
    }

    public boolean ping() {
        return write(ReplType.ping, new Ping(ConfForSlot.global.netListenAddresses));
    }

    public boolean bye() {
        System.out.println("Send bye to server: " + replPair.getHostAndPort() + ", slot: " + slot);
        return write(ReplType.bye, new Ping(ConfForSlot.global.netListenAddresses));
    }

    public void connect(String host, int port, Callable<ByteBuf> connectedCallback) {
        TcpSocket.connect(netWorkerEventloop, new InetSocketAddress(host, port))
                .whenResult(socket -> {
                    log.info("Connected to server at {}:{}, slot: {}", host, port, slot);
                    sock = socket;

                    BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                            .decodeStream(new RequestDecoder())
                            .map(pipeline -> {
                                if (pipeline == null) {
                                    return null;
                                }

                                // no flush pipeline for repl
                                var request = pipeline.getFirst();
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

                    if (connectedCallback != null) {
                        sock.write(connectedCallback.call());
                    }
                })
                .whenException(e -> log.error("Could not connect to server, to server: " + host + ":" + port + ", slot: " + slot, e));
    }

    public void close() {
        if (sock != null && !sock.isClosed()) {
            sock.close();
            System.out.println("Closed socket, to server: " + replPair.getHostAndPort() + ", slot: " + slot);
        } else {
            System.out.println("Socket is already closed, to server: " + replPair.getHostAndPort() + ", slot: " + slot);
        }
    }
}
