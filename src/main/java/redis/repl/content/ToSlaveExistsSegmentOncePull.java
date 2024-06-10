package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.ConfForSlot;
import redis.persist.OneSlot;
import redis.repl.ReplContent;

public class ToSlaveExistsSegmentOncePull implements ReplContent {
    public static final byte FLAG_IS_MY_CHARGE = 3;

    private final OneSlot oneSlot;
    private final ToMasterExistsSegmentMeta.OncePull oncePull;


    public ToSlaveExistsSegmentOncePull(OneSlot oneSlot, ToMasterExistsSegmentMeta.OncePull oncePull) {
        this.oneSlot = oneSlot;
        this.oncePull = oncePull;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte(FLAG_IS_MY_CHARGE);

        toBuf.writeInt(oncePull.beginSegmentIndex());
        toBuf.writeInt(oncePull.segmentCount());
        // seq long
        oneSlot.getSegmentMergeFlagListBatchForRepl(oncePull.beginSegmentIndex(), oncePull.segmentCount())
                .forEach(toBuf::writeLong);
        toBuf.writeInt(oncePull.walGroupIndex());

        // read 4M data and send to slave
        var bytes = oneSlot.preadForRepl(oncePull.beginSegmentIndex());
        toBuf.write(bytes);
    }

    @Override
    public int encodeLength() {
        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        // flag byte, begin segment index int, segment count int
        // seq long * segment count, segment length * segment count
        return 1 + 4 + 4 + 8 * oncePull.segmentCount() + segmentLength * oncePull.segmentCount();
    }
}
