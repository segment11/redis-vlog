package redis.persist;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.ConfForSlot;
import redis.KeyHash;
import redis.SnowFlake;
import redis.metric.SimpleGauge;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static redis.persist.Chunk.SEGMENT_HEADER_LENGTH;

public class SegmentBatch {
    private final byte[] bytes;
    private final ByteBuffer buffer;
    private final byte slot;
    private final String slotStr;

    private final int chunkSegmentLength;
    private final SnowFlake snowFlake;

    final static SimpleGauge segmentBatchGauge = new SimpleGauge("segment_batch", "chunk segment compress",
            "slot");

    static {
        segmentBatchGauge.register();
    }

    long segmentCompressCountTotal;
    private long segmentCompressTimeTotalUs;

    long compressBytesTotal;
    private long compressedBytesTotal;

    long batchSegmentCountTotal;
    private long batchKvCountTotal;

    private long beforeTightSegmentCountTotal;
    long afterTightSegmentCountTotal;

    private final Logger log = LoggerFactory.getLogger(SegmentBatch.class);

    public SegmentBatch(byte slot, SnowFlake snowFlake) {
        this.chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;
        this.slot = slot;
        this.slotStr = String.valueOf(slot);

        this.bytes = new byte[chunkSegmentLength];
        this.buffer = ByteBuffer.wrap(bytes);

        this.snowFlake = snowFlake;

        this.initMetricsCollect();
    }

    private void initMetricsCollect() {
        segmentBatchGauge.addRawGetter(() -> {
            var labelValues = List.of(slotStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            if (segmentCompressCountTotal > 0) {
                map.put("segment_compress_time_total_us", new SimpleGauge.ValueWithLabelValues((double) segmentCompressTimeTotalUs, labelValues));
                map.put("segment_compress_count_total", new SimpleGauge.ValueWithLabelValues((double) segmentCompressCountTotal, labelValues));
                map.put("segment_compress_time_avg_us", new SimpleGauge.ValueWithLabelValues((double) segmentCompressTimeTotalUs / segmentCompressCountTotal, labelValues));
            }

            if (compressBytesTotal > 0) {
                map.put("compress_bytes_total", new SimpleGauge.ValueWithLabelValues((double) compressBytesTotal, labelValues));
                map.put("compressed_bytes_total", new SimpleGauge.ValueWithLabelValues((double) compressedBytesTotal, labelValues));
                map.put("compress_ratio", new SimpleGauge.ValueWithLabelValues((double) compressedBytesTotal / compressBytesTotal, labelValues));
            }

            if (batchSegmentCountTotal > 0) {
                map.put("batch_segment_count_total", new SimpleGauge.ValueWithLabelValues((double) batchSegmentCountTotal, labelValues));
                map.put("batch_kv_count_total", new SimpleGauge.ValueWithLabelValues((double) batchKvCountTotal, labelValues));
                map.put("batch_kv_count_avg", new SimpleGauge.ValueWithLabelValues((double) batchKvCountTotal / batchSegmentCountTotal, labelValues));
            }

            if (afterTightSegmentCountTotal > 0) {
                map.put("before_tight_segment_count_total", new SimpleGauge.ValueWithLabelValues((double) beforeTightSegmentCountTotal, labelValues));
                map.put("after_tight_segment_count_total", new SimpleGauge.ValueWithLabelValues((double) afterTightSegmentCountTotal, labelValues));
                map.put("tight_segment_ratio", new SimpleGauge.ValueWithLabelValues((double) afterTightSegmentCountTotal / beforeTightSegmentCountTotal, labelValues));
            }

            return map;
        });
    }

    record SegmentCompressedBytesWithIndex(byte[] compressedBytes, int segmentIndex, long segmentSeq) {
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
    public static final int MAX_BLOCK_NUMBER = 4;
    // seq long + total bytes length int + each sub block * (offset short + length short)
    private static final int HEADER_LENGTH = 8 + 4 + MAX_BLOCK_NUMBER * (2 + 2);

    public static int subBlockMetaPosition(int subBlockIndex) {
        return 8 + 4 + subBlockIndex * (2 + 2);
    }

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
        beforeTightSegmentCountTotal += segments.size();

        ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> r = new ArrayList<>(segments.size());

        ArrayList<SegmentCompressedBytesWithIndex> onceList = new ArrayList<>(MAX_BLOCK_NUMBER);
        int onceListBytesLength = 0;

        int afterTightSegmentIndex = segments.get(0).segmentIndex;
        for (int i = 0; i < segments.size(); i++) {
            var segment = segments.get(i);
            var compressedBytes = segment.compressedBytes;

            if (onceList.size() == MAX_BLOCK_NUMBER || onceListBytesLength + compressedBytes.length > chunkSegmentLength - HEADER_LENGTH) {
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

        afterTightSegmentCountTotal += r.size();
        return r;
    }

    public ArrayList<SegmentTightBytesWithLengthAndSegmentIndex> splitAndTight(ArrayList<Wal.V> list, int[] nextNSegmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        ArrayList<SegmentCompressedBytesWithIndex> result = new ArrayList<>(100);
        ArrayList<Wal.V> onceList = new ArrayList<>(100);

        int i = 0;

        var persistLength = SEGMENT_HEADER_LENGTH;
        for (Wal.V v : list) {
            persistLength += v.persistLength();

            if (persistLength < chunkSegmentLength) {
                onceList.add(v);
            } else {
                if (i >= nextNSegmentIndex.length) {
                    log.warn("Batch next {} segment prepare is not enough, list size: {}", nextNSegmentIndex.length, list.size());
                    throw new IllegalArgumentException("Batch next " + nextNSegmentIndex.length + " segment prepare is not enough, list size: " + list.size());
                }

                result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
                i++;

                onceList.clear();
                persistLength = SEGMENT_HEADER_LENGTH + v.persistLength();
                onceList.add(v);
            }
        }

        if (!onceList.isEmpty()) {
            if (i >= nextNSegmentIndex.length) {
                log.warn("Batch next {} segment prepare is not enough, list size: {}", nextNSegmentIndex.length, list.size());
                throw new IllegalArgumentException("Batch next " + nextNSegmentIndex.length + " segment prepare is not enough, list size: " + list.size());
            }

            result.add(compressAsSegment(onceList, nextNSegmentIndex[i], returnPvmList));
        }

        return tight(result, returnPvmList);
    }

    private SegmentCompressedBytesWithIndex compressAsSegment(ArrayList<Wal.V> list, int segmentIndex, ArrayList<PersistValueMeta> returnPvmList) {
        batchSegmentCountTotal++;
        batchKvCountTotal += list.size();

        long segmentSeq = snowFlake.nextId();

        // only use key bytes hash to calculate crc
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
            buffer.putShort((short) keyBytes.length);
            buffer.put(keyBytes);
            buffer.put(v.cvEncoded());

            int length = v.persistLength();

            var pvm = new PersistValueMeta();
            pvm.keyBytes = keyBytes;
            pvm.keyHash = v.keyHash();
            pvm.bucketIndex = v.bucketIndex();
            pvm.isFromMerge = v.isFromMerge();

            pvm.slot = slot;
            // tmp 0, then update
            pvm.subBlockIndex = 0;
            pvm.length = length;
            // tmp current segment index, then update
            pvm.segmentIndex = segmentIndex;
            pvm.segmentOffset = offsetInThisSegment;
            pvm.expireAt = v.expireAt();
            pvm.seq = v.seq();
            returnPvmList.add(pvm);

            offsetInThisSegment += length;
        }

        if (buffer.remaining() >= 2) {
            // write 0 short, so merge loop can break, because reuse old bytes
            buffer.putShort((short) 0);
        }

        // update crc
        int segmentCrc32 = KeyHash.hash32(crcCalBytes);
        // refer to SEGMENT_HEADER_LENGTH definition
        // seq long + cv number int + crc int
        buffer.putInt(8 + 4, segmentCrc32);

        // important: 4KB decompress cost ~200us, so use 4KB segment length for better read latency
        // double compress

        segmentCompressCountTotal++;
        var beginT = System.nanoTime();
        var compressedBytes = Zstd.compress(bytes);
        var costT = (System.nanoTime() - beginT) / 1000;
        segmentCompressTimeTotalUs += costT;
        compressBytesTotal += bytes.length;
        compressedBytesTotal += compressedBytes.length;

        buffer.clear();
        Arrays.fill(bytes, (byte) 0);

        return new SegmentCompressedBytesWithIndex(compressedBytes, segmentIndex, segmentSeq);
    }

    public interface CvCallback {
        void callback(String key, CompressedValue cv, int offsetInThisSegment);
    }

    static class ForDebugCvCallback implements CvCallback {
        @Override
        public void callback(String key, CompressedValue cv, int offsetInThisSegment) {
            System.out.println("key: " + key + ", cv: " + cv + ", offsetInThisSegment: " + offsetInThisSegment);
        }
    }

    public static void iterateFromSegmentBytes(byte[] decompressedBytes, CvCallback cvCallback) {
        var buf = Unpooled.wrappedBuffer(decompressedBytes);
        // for crc check
        var segmentSeq = buf.readLong();
        var cvCount = buf.readInt();
        var segmentCrc32 = buf.readInt();

        int offsetInThisSegment = Chunk.SEGMENT_HEADER_LENGTH;
        while (true) {
            // refer to comment: write 0 short, so merge loop can break, because reuse old bytes
            if (buf.readableBytes() < 2) {
                break;
            }

            var keyLength = buf.readShort();
            if (keyLength == 0) {
                break;
            }

            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length: " + keyLength);
            }

            var keyBytes = new byte[keyLength];
            buf.readBytes(keyBytes);
            var key = new String(keyBytes);

            var cv = CompressedValue.decode(buf, keyBytes, 0);
            int length = Wal.V.persistLength(keyLength, cv.encodedLength());

            cvCallback.callback(key, cv, offsetInThisSegment);

            offsetInThisSegment += length;
        }
    }
}
