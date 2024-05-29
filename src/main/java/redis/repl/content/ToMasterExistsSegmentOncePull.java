package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class ToMasterExistsSegmentOncePull implements ReplContent {
    public static final byte FLAG_IS_MY_CHARGE = 2;

    private final ToMasterExistsSegmentMeta.OncePull oncePull;

    public ToMasterExistsSegmentOncePull(ToMasterExistsSegmentMeta.OncePull oncePull) {
        this.oncePull = oncePull;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte(FLAG_IS_MY_CHARGE);

        toBuf.writeInt(oncePull.beginSegmentIndex());
        toBuf.writeInt(oncePull.segmentCount());
    }

    @Override
    public int encodeLength() {
        // flag byte, begin segment index int, segment count int
        return 1 + ToMasterExistsSegmentMeta.OncePull.ENCODED_LENGTH;
    }
}
