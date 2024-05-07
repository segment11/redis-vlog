package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

import java.util.ArrayList;
import java.util.List;

public class ToSlaveSegmentWrite implements ReplContent {
    private final byte workerId;
    private final byte batchIndex;
    private final int segmentLength;
    private final int segmentIndex;
    private final int segmentCount;
    private final List<Long> segmentSeqList;
    private final byte[] bytes;
    private final int capacity;

    public ToSlaveSegmentWrite(byte workerId, byte batchIndex, int segmentLength, int segmentIndex, int segmentCount,
                               List<Long> segmentSeqList, byte[] bytes, int capacity) {
        this.workerId = workerId;
        this.batchIndex = batchIndex;
        this.segmentLength = segmentLength;
        this.segmentIndex = segmentIndex;
        this.segmentCount = segmentCount;
        this.segmentSeqList = segmentSeqList;
        this.bytes = bytes;
        this.capacity = capacity;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte(workerId);
        toBuf.writeByte(batchIndex);
        toBuf.writeInt(segmentLength);
        toBuf.writeInt(segmentIndex);
        toBuf.writeInt(segmentCount);
        for (long seq : segmentSeqList) {
            toBuf.writeLong(seq);
        }
        toBuf.writeInt(bytes.length);
        toBuf.write(bytes);
        toBuf.writeInt(capacity);
    }

    @Override
    public int encodeLength() {
        return 1 + 1 + 4 + 4 + 4 + 8 * segmentSeqList.size() + 4 + bytes.length + 4;
    }

}
