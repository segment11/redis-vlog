package redis.repl.content;

import redis.repl.ReplContent;

public class Ping implements ReplContent {
    private final String netListenAddresses;

    public Ping(String netListenAddresses) {
        this.netListenAddresses = netListenAddresses;
    }

    @Override
    public byte[] encode() {
        return netListenAddresses.getBytes();
    }
}
