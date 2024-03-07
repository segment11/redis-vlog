package redis.persist;

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressStats;
import redis.ConfForSlot;
import redis.KeyHash;
import redis.SnowFlake;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static redis.CompressedValue.KEY_HEADER_LENGTH;
import static redis.CompressedValue.VALUE_HEADER_LENGTH;
import static redis.persist.Chunk.*;

public class SegmentBatch {
    private final byte[] bytes;
    private final ByteBuffer buffer;

    private final byte workerId;
    private final byte slot;
    private final byte batchIndex;
    private final int segmentLength;
    private final SnowFlake snowFlake;

    private final CompressStats compressStats;

    private final Logger log = LoggerFactory.getLogger(SegmentBatch.class);

    public SegmentBatch(byte workerId, byte slot, byte batchIndex, SnowFlake snowFlake) {
        this.segmentLength = ConfForSlot.global.confChunk.segmentLength;

        this.workerId = workerId;
        this.slot = slot;
        this.batchIndex = batchIndex;
        this.bytes = new byte[segmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;

        this.compressStats = new CompressStats("w-" + workerId + "-s-" + slot);
    }

    public record SegmentCompressedBytesWithIndex(byte[] compressedBytesWithLength, int segmentIndex) {
        @Override
        public String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "segmentIndex=" + segmentIndex +
                    ", c.length=" + compressedBytesWithLength.length +
                    '}';
        }
    }

    public ArrayList<SegmentCompressedBytesWithIndex> split(ArrayList<Wal.V> list, int[] nextNSegmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentCompressedBytesWithIndex> result = new ArrayList<>(100);
        ArrayList<Wal.V> onceList = new ArrayList<>(100);

        int i = 0;

        var persistLength = SEGMENT_HEADER_LENGTH;
        for (Wal.V v : list) {
            persistLength += v.persistLength();

            if (persistLength < segmentLength) {
                onceList.add(v);
            } else {
                if (i >= ONCE_PREPARE_SEGMENT_COUNT) {
                    log.warn("Batch next {} segment prepare is not enough.", ONCE_PREPARE_SEGMENT_COUNT);
                    return null;
//                    throw new IllegalArgumentException("Batch next " + ONCE_PREPARE_SEGMENT_COUNT + " segment prepare is not enough.");
                }

                result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
                i++;

                onceList.clear();
                persistLength = SEGMENT_HEADER_LENGTH + v.persistLength();
                onceList.add(v);
            }
        }

        if (!onceList.isEmpty()) {
            if (i >= ONCE_PREPARE_SEGMENT_COUNT) {
                log.warn("Batch next {} segment prepare is not enough.", ONCE_PREPARE_SEGMENT_COUNT);
                return null;
//                    throw new IllegalArgumentException("Batch next " + ONCE_PREPARE_SEGMENT_COUNT + " segment prepare is not enough.");
            }

            result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
        }
        return result;
    }

    private SegmentCompressedBytesWithIndex compressAsSegment(ArrayList<Wal.V> list, int segmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        long segmentSeq = snowFlake.nextId();

        // only use key bytes mask value to calculate crc
        var crcCalBytes = new byte[8 * list.size()];
        var crcCalBuffer = ByteBuffer.wrap(crcCalBytes);

        // write segment header
        buffer.clear();
        buffer.putLong(segmentSeq);
        buffer.putInt(list.size());
        // temp write crc, then update
        buffer.putInt(0);

        final long offsetThisSegment = (long) segmentIndex * segmentLength;
        int offsetInThisSegment = SEGMENT_HEADER_LENGTH;

        for (var v : list) {
            crcCalBuffer.putLong(v.keyHash());

            var keyBytes = v.key().getBytes();
            buffer.put((byte) keyBytes.length);
            buffer.put(keyBytes);
            buffer.put(v.cvEncoded());

            int lenKey = KEY_HEADER_LENGTH + keyBytes.length;
            int lenValue = VALUE_HEADER_LENGTH + v.cvEncodedLength();
            int length = lenKey + lenValue;

            var pvm = new PersistValueMeta();
            pvm.keyBytes = keyBytes;
            pvm.keyHash = v.keyHash();
            pvm.bucketIndex = v.bucketIndex();

            pvm.workerId = workerId;
            pvm.slot = slot;
            pvm.batchIndex = batchIndex;
            pvm.length = length;
            pvm.offset = offsetThisSegment + offsetInThisSegment;
            pvm.expireAt = v.expireAt();
            returnPvmList.add(pvm);

            offsetInThisSegment += length;
        }

        if (buffer.remaining() >= 4) {
            // write 0 int, so merge loop can break, because reuse old bytes
            buffer.putInt(0);
        }

        // update crc
        int segmentMaskedValue = KeyHash.hash32(crcCalBytes);
        // refer to SEGMENT_HEADER_LENGTH definition
        // seq long + cv number int + crc int
        buffer.putInt(8 + 4, segmentMaskedValue);

        // important: 4KB decompress cost ~200us, so use 4KB segment length for better read latency
        // double compress

        long beginT2 = System.nanoTime();
        var compressedBytes = Zstd.compress(bytes, COMPRESS_LEVEL);
        long costT2 = System.nanoTime() - beginT2;

        // stats
        compressStats.compressedCount++;
        compressStats.rawValueBodyTotalLength += segmentLength;
        compressStats.compressedValueBodyTotalLength += compressedBytes.length;
        compressStats.compressedCostTotalTimeNanos += costT2;

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        // copy again, perf bad
        var compressedBytesWithLength = new byte[compressedBytes.length + 4];
        ByteBuffer.wrap(compressedBytesWithLength).putInt(compressedBytes.length).put(compressedBytes);

        return new SegmentCompressedBytesWithIndex(compressedBytesWithLength, segmentIndex);
    }
}
