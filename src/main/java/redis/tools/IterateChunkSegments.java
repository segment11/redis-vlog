package redis.tools;

import com.github.luben.zstd.Zstd;
import redis.ConfForSlot;
import redis.persist.ChunkMergeJob;
import redis.persist.SegmentBatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class IterateChunkSegments {
    public static void main(String[] args) throws IOException {
        ConfForSlot.global = ConfForSlot.c1m;

        byte slot = 0;
        int[] sumArray = new int[ConfForSlot.global.confChunk.maxSegmentNumber()];

        var slotDir = new File(persistDir + "/slot-" + slot);
        var chunkFiles = slotDir.listFiles((dir, name) -> name.startsWith("chunk-data-"));

        for (int i = 0; i < chunkFiles.length; i++) {
            iterateOneChunkFile(slot, (byte) i, chunkFiles[i], sumArray);
        }

        int sumTotal = 0;
        for (int i = 0; i < sumArray.length; i++) {
            sumTotal += sumArray[i];
        }
        System.out.println("sum total: " + sumTotal);
    }

    //    private static String persistDir = "/tmp/redis-vlog-test-data";
    private static String persistDir = "/tmp/redis-vlog/persist";

    public static void iterateOneChunkFile(byte slot, byte index, File chunkFile, int[] sumArray) throws IOException {
        if (chunkFile.length() == 0) {
            return;
        }

        var segmentLength = ConfForSlot.global.confChunk.segmentLength;
        var segmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;

        var beginSegmentIndex = index * segmentNumberPerFd;

        final var bytes = new byte[4096];
        var fis = new FileInputStream(chunkFile);
        int segmentIndexThisFd = 0;
        while (fis.read(bytes) != -1) {
            var segmentIndex = segmentIndexThisFd + beginSegmentIndex;

            ArrayList<ChunkMergeJob.CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(1000);

            var buffer = ByteBuffer.wrap(bytes);
            for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
                // position to target sub block
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
                });
            }

            for (var one : cvList) {
                System.out.println(one.shortString());
            }

            sumArray[segmentIndex] = cvList.size();

            segmentIndexThisFd++;
        }
        fis.close();
    }
}
