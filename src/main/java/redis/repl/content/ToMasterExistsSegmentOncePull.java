package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class ToMasterExistsSegmentOncePull implements ReplContent {
    public static final byte FLAG_IS_MY_CHARGE = 2;

    private final byte workerId;
    private final byte batchIndex;
    private final ToMasterExistsSegmentMeta.OncePull oncePull;

    public ToMasterExistsSegmentOncePull(byte workerId, byte batchIndex, ToMasterExistsSegmentMeta.OncePull oncePull) {
        this.workerId = workerId;
        this.batchIndex = batchIndex;
        this.oncePull = oncePull;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte(FLAG_IS_MY_CHARGE);
        toBuf.writeByte(workerId);
        toBuf.writeByte(batchIndex);

        toBuf.writeInt(oncePull.beginSegmentIndex());
        toBuf.writeInt(oncePull.segmentCount());
    }

    @Override
    public int encodeLength() {
        // flag byte, worker id byte, batch index byte, begin segment index int, segment count int
        return 1 + 1 + 1 + ToMasterExistsSegmentMeta.OncePull.ENCODED_LENGTH;
    }
}
