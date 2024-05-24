package redis.persist;

import com.github.luben.zstd.Zstd;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.KeyHash;
import redis.SnowFlake;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static redis.CompressedValue.KEY_HEADER_LENGTH;
import static redis.CompressedValue.VALUE_HEADER_LENGTH;
import static redis.persist.Chunk.ONCE_PREPARE_SEGMENT_COUNT;
import static redis.persist.Chunk.SEGMENT_HEADER_LENGTH;

public class SegmentBatch {
    private final byte[] bytes;
    private final ByteBuffer buffer;

    private final byte workerId;
    private final byte slot;
    private final byte batchIndex;

    private final String workerIdStr;
    private final String slotStr;

    private final int segmentLength;
    private final SnowFlake snowFlake;

    private static final Summary compressTimeSummary = Summary.build().name("compress_time").
            help("segment batch compress time summary").
            labelNames("worker_id", "slot").
            quantile(0.5, 0.05).
            quantile(0.9, 0.01).
            quantile(0.99, 0.01).
            quantile(0.999, 0.001)
            .register();

    private static final Counter compressBytesCounter = Counter.build().name("compress_bytes").
            help("segment batch compress bytes").
            labelNames("worker_id", "slot")
            .register();

    private static final Counter compressedBytesCounter = Counter.build().name("compressed_bytes").
            help("segment batch compressed bytes").
            labelNames("worker_id", "slot")
            .register();

    private final Logger log = LoggerFactory.getLogger(SegmentBatch.class);

    public SegmentBatch(byte workerId, byte slot, byte batchIndex, SnowFlake snowFlake) {
        this.segmentLength = ConfForSlot.global.confChunk.segmentLength;

        this.workerId = workerId;
        this.slot = slot;
        this.batchIndex = batchIndex;

        this.workerIdStr = String.valueOf(workerId);
        this.slotStr = String.valueOf(slot);

        this.bytes = new byte[segmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;
    }

    private record SegmentCompressedBytesWithIndex(byte[] compressedBytes, int segmentIndex, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentCompressedBytesWithIndex{" +
                    "segmentIndex=" + segmentIndex +
                    ", segmentSeq=" + segmentSeq +
                    ", compressedBytes.length=" + compressedBytes.length +
                    '}';
        }
    }

    public record SegmentTightBytesWithLengthAndSegmentIndex(byte[] tightBytesWithLength, int segmentIndex,
                                                             byte blockNumber, long segmentSeq) {
        @Override
        public String toString() {
            return "SegmentTightBytesWithLengthAndSegmentIndex{" +
                    "segmentIndex=" + segmentIndex +
                    ", blockNumber=" + blockNumber +
                    ", segmentSeq=" + segmentSeq +
                    ", tightBytesWithLength.length=" + tightBytesWithLength.length +
                    '}';
        }
    }

    // zstd compress ratio usually < 0.25, max 4 blocks tight to one segment
    static final int MAX_BLOCK_NUMBER = 4;
    // seq long + total bytes length int + each sub block * (offset short / length short)
    private static final int HEADER_LENGTH = 8 + 4 + MAX_BLOCK_NUMBER * (2 + 2);

    private SegmentTightBytesWithLengthAndSegmentIndex tightSegments(int afterTightSegmentIndex, ArrayList<SegmentCompressedBytesWithIndex> onceList, ArrayList<PersistValueMeta> returnPvmList) {
        for (int j = 0; j < onceList.size(); j++) {
            var subBlockIndex = (byte) j;
            var s = onceList.get(j);

            for (var pvm : returnPvmList) {
                if (pvm.segmentIndex == s.segmentIndex) {
                    pvm.subBlockIndex = subBlockIndex;
                    pvm.segmentIndex = afterTightSegmentIndex;
                }
            }
        }

        var totalBytesN = HEADER_LENGTH;
        for (var s : onceList) {
            totalBytesN += s.compressedBytes.length;
        }

        var tightBytesWithLength = new byte[totalBytesN];
        var buffer = ByteBuffer.wrap(tightBytesWithLength);
        var segmentSeq = snowFlake.nextId();
        buffer.putLong(segmentSeq);
        buffer.putInt(totalBytesN);

        int offset = HEADER_LENGTH;
        for (int i = 0; i < onceList.size(); i++) {
            var s = onceList.get(i);
            var compressedBytes = s.compressedBytes;
            var length = compressedBytes.length;

            buffer.putShort((short) offset);
            buffer.putShort((short) length);

            buffer.mark();
            buffer.position(offset).put(compressedBytes);
            buffer.reset();

            offset += length;
        }

        return new SegmentTightBytesWithLengthAndSegmentIndex(tightBytesWithLength, afterTightSegmentIndex, (byte) onceList.size(), segmentSeq);
    }

    private ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> tight(ArrayList<SegmentCompressedBytesWithIndex> segments, ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> r = new ArrayList<>(segments.size());

        ArrayList<SegmentCompressedBytesWithIndex> onceList = new ArrayList<>(MAX_BLOCK_NUMBER);
        int onceListBytesLength = 0;

        int afterTightSegmentIndex = segments.get(0).segmentIndex;
        for (int i = 0; i < segments.size(); i++) {
            var segment = segments.get(i);
            var compressedBytes = segment.compressedBytes;

            if (onceList.size() == MAX_BLOCK_NUMBER || onceListBytesLength + compressedBytes.length > segmentLength - HEADER_LENGTH) {
                var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
                r.add(tightOne);
                afterTightSegmentIndex++;

                onceList.clear();
                onceListBytesLength = 0;
            }

            onceList.add(segment);
            onceListBytesLength += compressedBytes.length;
        }

        if (!onceList.isEmpty()) {
            var tightOne = tightSegments(afterTightSegmentIndex, onceList, returnPvmList);
            r.add(tightOne);
        }

        return r;
    }

    public ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> splitAndTight(ArrayList<Wal.V> list, int[] nextNSegmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
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

        return tight(result, returnPvmList);
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
            // tmp 0, then update
            pvm.subBlockIndex = 0;
            pvm.length = length;
            // tmp current segment index, then update
            pvm.segmentIndex = segmentIndex;
            pvm.segmentOffset = offsetInThisSegment;
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

        var timer = compressTimeSummary.labels(workerIdStr, slotStr).startTimer();
        var compressedBytes = Zstd.compress(bytes);
        compressBytesCounter.labels(workerIdStr, slotStr).inc(bytes.length);
        compressedBytesCounter.labels(workerIdStr, slotStr).inc(compressedBytes.length);
        timer.observeDuration();

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentCompressedBytesWithIndex(compressedBytes, segmentIndex, segmentSeq);
    }
}
