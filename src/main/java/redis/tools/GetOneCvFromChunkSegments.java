package redis.tools;

import com.github.luben.zstd.Zstd;
import redis.ConfForSlot;
import redis.persist.ChunkMergeJob;
import redis.persist.SegmentBatch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class GetOneCvFromChunkSegments {
    public static void main(String[] args) throws IOException {
        ConfForSlot.global = ConfForSlot.c1m;

        byte slot = 0;

        final int segmentIndex = 9333;
        final int segmentOffset = 716;
        final String toFindKey = "key:000000369711";

        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        var segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
        var chunkFileIndex = segmentIndex / segmentNumberPerFd;
        var segmentIndexInChunkFile = segmentIndex % segmentNumberPerFd;

        var slotDir = new File(persistDir + "/slot-" + slot);
        var bytes = new byte[segmentLength];

        var chunkFile = new File(slotDir, "chunk-data-" + chunkFileIndex);
        if (!chunkFile.exists()) {
            throw new IllegalStateException("Chunk file not exists: " + chunkFile);
        }

        var raf = new RandomAccessFile(chunkFile, "r");
        raf.seek(segmentIndexInChunkFile * segmentLength);
        var n = raf.read(bytes);
        if (n != segmentLength) {
            throw new IllegalStateException("Read error, n=" + n + ", segment length=" + segmentLength);
        }
        raf.close();

        ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(1000);

        var buffer = ByteBuffer.wrap(bytes);
        for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
            buffer.position(SegmentBatch.subBlockMetaPosition(subBlockIndex));
            var subBlockOffset = buffer.getShort();
            if (subBlockOffset == 0) {
                break;
            }

            var subBlockLength = buffer.getShort();

            var decompressedBytes = new byte[segmentLength];
            var d = Zstd.decompressByteArray(decompressedBytes, 0, segmentLength, bytes, subBlockOffset, subBlockLength);
            if (d != segmentLength) {
                throw new IllegalStateException("Decompress error, s=" + slot
                        + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", segmentLength=" + segmentLength);
            }

            int finalSubBlockIndex = subBlockIndex;
            SegmentBatch.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                cvList.add(new ChunkMergeJob.CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));

                if (segmentOffset == offsetInThisSegment) {
                    System.out.println("key: " + new String(key) + ", cv: " + cv);
                }

                if (toFindKey.equals(new String(key))) {
                    System.out.println("key: " + new String(key) + ", cv: " + cv);
                }
            });
        }

        for (var one : cvList) {
            System.out.println(one.shortString() + ", value length=" + one.cv.compressedLength());
        }
    }

    //    private static String persistDir = "/tmp/redis-vlog-test-data";
    private static String persistDir = "/tmp/redis-vlog/persist";
}
