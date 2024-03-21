package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class Pong implements ReplContent {
    private final String netListenAddresses;

    public Pong(String netListenAddresses) {
        this.netListenAddresses = netListenAddresses;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.put(netListenAddresses.getBytes());
    }

    @Override
    public int encodeLength() {
        return netListenAddresses.length();
    }
}
