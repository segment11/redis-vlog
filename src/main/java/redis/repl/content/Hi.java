package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.Binlog;
import redis.repl.ReplContent;

public class Hi implements ReplContent {
    private final long slaveUuid;
    private final long masterUuid;
    private final Binlog.FileIndexAndOffset binlogFileIndexAndOffset;
    private final Binlog.FileIndexAndOffset earliestFileIndexAndOffset;

    public Hi(long slaveUuid, long masterUuid, Binlog.FileIndexAndOffset binlogFileIndexAndOffset,
              Binlog.FileIndexAndOffset earliestFileIndexAndOffset) {
        this.slaveUuid = slaveUuid;
        this.masterUuid = masterUuid;
        this.binlogFileIndexAndOffset = binlogFileIndexAndOffset;
        this.earliestFileIndexAndOffset = earliestFileIndexAndOffset;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(slaveUuid);
        toBuf.writeLong(masterUuid);
        toBuf.writeInt(binlogFileIndexAndOffset.fileIndex());
        toBuf.writeLong(binlogFileIndexAndOffset.offset());
        toBuf.writeInt(earliestFileIndexAndOffset.fileIndex());
        toBuf.writeLong(earliestFileIndexAndOffset.offset());
    }

    @Override
    public int encodeLength() {
        return 8 + 8 + 4 + 8 + 4 + 8;
    }
}
