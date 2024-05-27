package redis.persist;

import redis.SnowFlake;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class ChunkMerger {
    public static final byte MAX_MERGE_WORKERS = 32;
    public static final byte MAX_TOP_MERGE_WORKERS = 32;

    private final ChunkMergeWorker[] chunkMergeWorkers;
    private final ChunkMergeWorker[] topChunkMergeWorkers;

    private final short slotNumber;
    private final byte requestWorkers;
    private final byte mergeWorkers;
    private final byte topMergeWorkers;
    private final SnowFlake snowFlake;

    private int compressLevel;

    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    public ChunkMerger(byte requestWorkers, byte mergeWorkers, byte topMergeWorkers, short slotNumber, SnowFlake snowFlake) {
        this.slotNumber = slotNumber;
        this.requestWorkers = requestWorkers;
        this.mergeWorkers = mergeWorkers;
        this.topMergeWorkers = topMergeWorkers;
        this.snowFlake = snowFlake;

        this.chunkMergeWorkers = new ChunkMergeWorker[mergeWorkers];
        for (int i = 0; i < mergeWorkers; i++) {
            this.chunkMergeWorkers[i] = new ChunkMergeWorker((byte) (requestWorkers + i), slotNumber,
                    requestWorkers, mergeWorkers, topMergeWorkers,
                    snowFlake, this);
            this.chunkMergeWorkers[i].initEventloop(false);

            this.chunkMergeWorkers[i].compressLevel = compressLevel;
        }

        this.topChunkMergeWorkers = new ChunkMergeWorker[topMergeWorkers];
        for (int i = 0; i < topMergeWorkers; i++) {
            this.topChunkMergeWorkers[i] = new ChunkMergeWorker((byte) (requestWorkers + mergeWorkers + i), slotNumber,
                    requestWorkers, mergeWorkers, topMergeWorkers,
                    snowFlake, this);
            this.topChunkMergeWorkers[i].initEventloop(true);

            this.topChunkMergeWorkers[i].compressLevel = compressLevel;
        }
    }

    public ChunkMergeWorker getChunkMergeWorker(byte mergeWorkerId) {
        for (var worker : chunkMergeWorkers) {
            if (worker.mergeWorkerId == mergeWorkerId) {
                return worker;
            }
        }

        for (var worker : topChunkMergeWorkers) {
            if (worker.mergeWorkerId == mergeWorkerId) {
                return worker;
            }
        }
        return null;
    }

    public void start() {
        for (var worker : chunkMergeWorkers) {
            worker.start();
        }
        for (var topWorker : topChunkMergeWorkers) {
            topWorker.start();
        }
    }

    public void stop() {
        for (var worker : chunkMergeWorkers) {
            worker.stop();
        }
        for (var topWorker : topChunkMergeWorkers) {
            topWorker.stop();
        }
    }

    CompletableFuture<Integer> submit(byte workerId, byte slot, byte batchIndex, ArrayList<Integer> needMergeSegmentIndexList) {
        var job = new ChunkMergeWorker.Job();
        job.workerId = workerId;
        job.slot = slot;
        job.batchIndex = batchIndex;
        job.needMergeSegmentIndexList = needMergeSegmentIndexList;
        job.snowFlake = snowFlake;

        // begin with 0
        // merge worker handle request worker chunks
        if (workerId < requestWorkers) {
            // by slot
            int chooseIndex = slot % chunkMergeWorkers.length;
            var mergeWorker = chunkMergeWorkers[chooseIndex];
            job.mergeWorker = mergeWorker;
            return mergeWorker.submit(job);
        } else {
            // top merge worker handle self chunks
            if (workerId >= requestWorkers + chunkMergeWorkers.length) {
                for (var topMergeWorker : topChunkMergeWorkers) {
                    if (topMergeWorker.mergeWorkerId == workerId) {
                        job.mergeWorker = topMergeWorker;
                        // use current thread when top merge worker merge self
                        // wait or post later
                        return CompletableFuture.completedFuture(job.get());
                    }
                }
            } else {
                // top merge worker handle merge worker chunks
                // by slot
                int chooseIndex = slot % topChunkMergeWorkers.length;
                var topMergeWorker = topChunkMergeWorkers[chooseIndex];
                job.mergeWorker = topMergeWorker;
                return topMergeWorker.submit(job);
            }
        }
        // should not reach here
        return CompletableFuture.completedFuture(job.validCvCountAfterRun);
    }
}
