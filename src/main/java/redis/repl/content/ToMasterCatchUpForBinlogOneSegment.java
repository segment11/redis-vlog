package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class ToMasterCatchUpForBinlogOneSegment implements ReplContent {
    private final long binlogMasterUuid;
    private final int binlogFileIndex;
    private final long binlogOffset;

    public ToMasterCatchUpForBinlogOneSegment(long binlogMasterUuid, int binlogFileIndex, long binlogOffset) {
        this.binlogMasterUuid = binlogMasterUuid;
        this.binlogFileIndex = binlogFileIndex;
        this.binlogOffset = binlogOffset;
    }


    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(binlogMasterUuid);
        toBuf.writeInt(binlogFileIndex);
        toBuf.writeLong(binlogOffset);
    }

    @Override
    public int encodeLength() {
        return 8 + 4 + 8;
    }
}
