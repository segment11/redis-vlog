package redis.repl.content;

import redis.repl.ReplContent;

public class Pong implements ReplContent {
    private final String netListenAddresses;

    public Pong(String netListenAddresses) {
        this.netListenAddresses = netListenAddresses;
    }

    @Override
    public byte[] encode() {
        return netListenAddresses.getBytes();
    }
}
