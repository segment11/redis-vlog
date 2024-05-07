package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

import java.util.List;

public class ToSlaveExistsSegmentMeta implements ReplContent {
    public static final byte FLAG_IS_MY_CHARGE = 1;

    private final byte workerId;
    private final byte batchIndex;
    private final List<ToMasterExistsSegmentMeta.OncePull> oncePulls;

    public ToSlaveExistsSegmentMeta(byte workerId, byte batchIndex, List<ToMasterExistsSegmentMeta.OncePull> oncePulls) {
        this.workerId = workerId;
        this.batchIndex = batchIndex;
        this.oncePulls = oncePulls;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte(FLAG_IS_MY_CHARGE);
        toBuf.writeByte(workerId);
        toBuf.writeByte(batchIndex);
        toBuf.writeInt(oncePulls.size());

        for (var oncePull : oncePulls) {
            toBuf.writeInt(oncePull.beginSegmentIndex());
            toBuf.writeInt(oncePull.segmentCount());
        }
    }

    @Override
    public int encodeLength() {
        // flag byte, worker id byte, batch index byte, once pull count int
        return 1 + 1 + 1 + 4 + (oncePulls.isEmpty() ? 0 : oncePulls.size() * ToMasterExistsSegmentMeta.OncePull.ENCODED_LENGTH);
    }
}
