package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.persist.Wal;
import redis.repl.ReplContent;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ToSlaveWalAppendBatch implements ReplContent {
    private final int sendOnceMaxCount;
    private final int sendOnceMaxLength;
    private final byte[] bytes;
    private final ByteBuffer buffer;

    // 2 bytes as short for batch count, 4 bytes as int for data length
    private static final int HEADER_LENGTH = 2 + 4;

    private short batchCount = 0;
    private int dataLength = 0;

    public void reset() {
        batchCount = 0;
        dataLength = 0;
        Arrays.fill(bytes, (byte) 0);
        buffer.position(HEADER_LENGTH);
    }

    public boolean isEmpty() {
        return batchCount == 0;
    }

    public ToSlaveWalAppendBatch(int sendOnceMaxCount, int sendOnceMaxLength) {
        this.sendOnceMaxCount = sendOnceMaxCount;
        this.sendOnceMaxLength = sendOnceMaxLength;

        this.bytes = new byte[sendOnceMaxLength];
        this.buffer = ByteBuffer.wrap(bytes);
        buffer.position(HEADER_LENGTH);
    }

    public record AddBatchResult(boolean needSent, boolean isLastIncluded) {

    }

    public AddBatchResult addBatch(byte batchIndex, boolean isValueShort, Wal.V v, int offset) {
        if (batchCount >= sendOnceMaxCount) {
            return new AddBatchResult(true, false);
        }

        int vEncodeLength = v.encodeLength();
        // 1 byte for batch index, 1 byte for is value short, 4 bytes as int for offset, 4 bytes as int for v encode length
        int itemLength = 1 + 1 + 4 + 4 + vEncodeLength;
        if (dataLength + itemLength > sendOnceMaxLength) {
            return new AddBatchResult(true, false);
        }

        buffer.put(batchIndex);
        buffer.put(isValueShort ? (byte) 1 : (byte) 0);
        buffer.putInt(offset);
        buffer.putInt(vEncodeLength);
        buffer.put(v.encode());

        batchCount++;
        dataLength += itemLength;

        buffer.putShort(0, batchCount);
        buffer.putInt(2, dataLength);

        return new AddBatchResult(batchCount >= sendOnceMaxCount, true);
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(bytes, 0, HEADER_LENGTH + dataLength);
    }

    @Override
    public int encodeLength() {
        return HEADER_LENGTH + dataLength;
    }

}
