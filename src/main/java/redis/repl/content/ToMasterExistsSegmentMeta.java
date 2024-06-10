package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.ConfForSlot;
import redis.persist.MetaChunkSegmentFlagSeq;
import redis.repl.ReplContent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ToMasterExistsSegmentMeta implements ReplContent {
    public static final byte FLAG_IS_MY_CHARGE = 0;

    private final byte[] metaBytes;

    public ToMasterExistsSegmentMeta(byte[] metaBytes) {
        this.metaBytes = metaBytes;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte(FLAG_IS_MY_CHARGE);
        toBuf.write(metaBytes);
    }

    @Override
    public int encodeLength() {
        // flag byte, meta bytes length
        return 1 + metaBytes.length;
    }

    // 4K one segment, 1024 segments means read 4M data and send to slave
    public static final int ONCE_SEGMENT_COUNT = 1024;

    public record OncePull(int beginSegmentIndex, int segmentCount, int walGroupIndex) {
        public static final int ENCODED_LENGTH = 4 + 4 + 4;

        public static OncePull decode(ByteBuffer buffer) {
            int beginSegmentIndex = buffer.getInt();
            int segmentCount = buffer.getInt();
            int walGroupIndex = buffer.getInt();
            return new OncePull(beginSegmentIndex, segmentCount, walGroupIndex);
        }
    }

    public static List<OncePull> diffMasterAndSlave(byte[] metaBytesMaster, byte[] contentBytesFromSlave) {
        // content bytes include flag byte
        if (contentBytesFromSlave.length != metaBytesMaster.length + 1) {
            throw new IllegalArgumentException("Repl exists segment meta from slave meta length is not equal to master meta length");
        }

        var oncePulls = new ArrayList<OncePull>();
        var maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();

        int length = ONCE_SEGMENT_COUNT * MetaChunkSegmentFlagSeq.ONE_LENGTH;
        for (int segmentIndex = 0; segmentIndex < maxSegmentNumber; segmentIndex += ONCE_SEGMENT_COUNT) {
            int beginSegmentIndex = segmentIndex;
            int offset = beginSegmentIndex * MetaChunkSegmentFlagSeq.ONE_LENGTH;

            var bufferMaster = ByteBuffer.wrap(metaBytesMaster, offset, length);
            var bufferSlave = ByteBuffer.wrap(contentBytesFromSlave, 1 + offset, length);

            if (bufferMaster.equals(bufferSlave)) {
                continue;
            }

            // todo
            // once pull group by walGroupIndex
            oncePulls.add(new OncePull(beginSegmentIndex, ONCE_SEGMENT_COUNT, 0));
        }

        return oncePulls;
    }
}
