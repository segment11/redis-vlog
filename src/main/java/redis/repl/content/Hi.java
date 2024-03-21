package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class Hi implements ReplContent {
    private final long slaveUuid;
    private final long masterUuid;

    public Hi(long slaveUuid, long masterUuid) {
        this.slaveUuid = slaveUuid;
        this.masterUuid = masterUuid;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(slaveUuid);
        toBuf.writeLong(masterUuid);
    }

    @Override
    public int encodeLength() {
        return 16;
    }
}
