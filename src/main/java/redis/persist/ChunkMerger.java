package redis.persist;

import io.activej.config.Config;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import redis.SnowFlake;
import redis.repl.MasterUpdateCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

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

    private final HashMap<Byte, MasterUpdateCallback> masterUpdateCallbackBySlot = new HashMap<>();

    void putMasterUpdateCallback(byte slot, MasterUpdateCallback masterUpdateCallback) {
        masterUpdateCallbackBySlot.put(slot, masterUpdateCallback);
    }

    private int compressLevel;

    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    // decompress is cpu intensive
    static final ThreadFactory persistWorkerThreadFactoryGroup1 = new AffinityThreadFactory("persist-worker-group1",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory persistWorkerThreadFactoryGroup2 = new AffinityThreadFactory("persist-worker-group2",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory persistWorkerThreadFactoryGroup3 = new AffinityThreadFactory("persist-worker-group3",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory persistWorkerThreadFactoryGroup4 = new AffinityThreadFactory("persist-worker-group4",
            AffinityStrategies.SAME_CORE);

    static final ThreadFactory mergeWorkerThreadFactoryGroup1 = new AffinityThreadFactory("merge-worker-group1",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory mergeWorkerThreadFactoryGroup2 = new AffinityThreadFactory("merge-worker-group2",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory mergeWorkerThreadFactoryGroup3 = new AffinityThreadFactory("merge-worker-group3",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory mergeWorkerThreadFactoryGroup4 = new AffinityThreadFactory("merge-worker-group4",
            AffinityStrategies.SAME_CORE);

    static final ThreadFactory topMergeWorkerThreadFactoryGroup1 = new AffinityThreadFactory("top-merge-worker-group1",
            AffinityStrategies.SAME_CORE);
    static final ThreadFactory topMergeWorkerThreadFactoryGroup2 = new AffinityThreadFactory("top-merge-worker-group2",
            AffinityStrategies.SAME_CORE);

    static ThreadFactory getPersistThreadFactoryForSlot(byte slot, short slotNumber) {
        final int step = 10;
        if (slotNumber <= step) {
            return persistWorkerThreadFactoryGroup1;
        }

        if (slotNumber <= step * 2) {
            return slot < step ? persistWorkerThreadFactoryGroup1 : persistWorkerThreadFactoryGroup2;
        }

        int diff = slot % 4;
        return switch (diff) {
            case 0 -> persistWorkerThreadFactoryGroup1;
            case 1 -> persistWorkerThreadFactoryGroup2;
            case 2 -> persistWorkerThreadFactoryGroup3;
            case 3 -> persistWorkerThreadFactoryGroup4;
            default -> throw new IllegalStateException("Unexpected value: " + diff);
        };
    }

    public ChunkMerger(byte requestWorkers, byte mergeWorkers, byte topMergeWorkers, short slotNumber,
                       SnowFlake snowFlake, Config chunkConfig) {
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
            this.chunkMergeWorkers[i].initExecutor(false);

            this.chunkMergeWorkers[i].compressLevel = compressLevel;
        }

        this.topChunkMergeWorkers = new ChunkMergeWorker[topMergeWorkers];
        for (int i = 0; i < topMergeWorkers; i++) {
            this.topChunkMergeWorkers[i] = new ChunkMergeWorker((byte) (requestWorkers + mergeWorkers + i), slotNumber,
                    requestWorkers, mergeWorkers, topMergeWorkers,
                    snowFlake, this);
            this.topChunkMergeWorkers[i].initExecutor(true);

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
        if (workerId < requestWorkers) {
            // by slot or by worker id?
            int chooseIndex = slot % chunkMergeWorkers.length;
            var mergeWorker = chunkMergeWorkers[chooseIndex];
            job.mergeWorker = mergeWorker;
            return mergeWorker.submit(job);
        } else {
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
