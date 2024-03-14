package redis.repl.content;

import redis.repl.ReplContent;

import java.nio.ByteBuffer;

public class Hi implements ReplContent {
    private final long slaveUuid;
    private final long masterUuid;

    public Hi(long slaveUuid, long masterUuid) {
        this.slaveUuid = slaveUuid;
        this.masterUuid = masterUuid;
    }

    @Override
    public byte[] encode() {
        var bytes = new byte[16];
        var buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(slaveUuid);
        buffer.putLong(masterUuid);
        return bytes;
    }
}
