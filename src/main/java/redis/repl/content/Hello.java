package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class Hello implements ReplContent {
    private final long slaveUuid;
    private final String netListenAddresses;

    public Hello(long slaveUuid, String netListenAddresses) {
        this.slaveUuid = slaveUuid;
        this.netListenAddresses = netListenAddresses;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(slaveUuid);
        toBuf.write(netListenAddresses.getBytes());
    }

    @Override
    public int encodeLength() {
        return 8 + netListenAddresses.length();
    }
}
