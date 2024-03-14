package redis.repl.content;

import redis.repl.ReplContent;

import java.nio.ByteBuffer;

public class Hello implements ReplContent {
    private final long slaveUuid;
    private final String netListenAddresses;

    public Hello(long slaveUuid, String netListenAddresses) {
        this.slaveUuid = slaveUuid;
        this.netListenAddresses = netListenAddresses;
    }

    @Override
    public byte[] encode() {
        byte[] b = netListenAddresses.getBytes();

        var bytes = new byte[8 + b.length];
        var buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(slaveUuid);
        buffer.put(b);
        return bytes;
    }
}
