package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.ConfForSlot;
import redis.persist.OneSlot;
import redis.repl.ReplContent;

public class ToSlaveExistsSegmentOncePull implements ReplContent {
    public static final byte FLAG_IS_MY_CHARGE = 3;

    private final OneSlot oneSlot;
    private final byte workerId;
    private final byte batchIndex;
    private final ToMasterExistsSegmentMeta.OncePull oncePull;

    public ToSlaveExistsSegmentOncePull(OneSlot oneSlot, byte workerId, byte batchIndex, ToMasterExistsSegmentMeta.OncePull oncePull) {
        this.oneSlot = oneSlot;
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
        // seq long
        oneSlot.getSomeSegmentsSeqList(workerId, batchIndex, oncePull.beginSegmentIndex(), oncePull.segmentCount())
                .forEach(toBuf::writeLong);

        // read 4M data and send to slave
        var bytes = oneSlot.preadForRepl(workerId, batchIndex, oncePull.beginSegmentIndex());
        toBuf.write(bytes);
    }

    @Override
    public int encodeLength() {
        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        // flag byte, worker id byte, batch index byte, begin segment index int, segment count int
        // seq long * segment count, segment length * segment count
        return 1 + 1 + 1 + 4 + 4 + 8 * oncePull.segmentCount() + segmentLength * oncePull.segmentCount();
    }
}
