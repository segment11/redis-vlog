package redis.persist;

import io.activej.config.Config;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.ConfForSlot;
import redis.Debug;
import redis.SnowFlake;
import redis.repl.MasterUpdateCallback;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

import static io.activej.config.converter.ConfigConverters.ofInteger;

public class ChunkMerger implements OfStats {
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

    private final Logger log = LoggerFactory.getLogger(getClass());

    private ScheduledExecutorService mergeSelfScheduledExecutor;

    private MetaTopChunkSegmentIndex metaTopChunkSegmentIndex;

    public void clearMetaTopChunkSegmentIndex() {
        metaTopChunkSegmentIndex.clear();
    }

    // read only, important
    public byte[] getMetaTopChunkSegmentIndexBytesForRepl() {
        return metaTopChunkSegmentIndex.getInMemoryCachedBytes();
    }

    public void overwriteMetaTopChunkSegmentIndexBytesFromRepl(byte[] bytes) {
        metaTopChunkSegmentIndex.overwriteInMemoryCachedBytes(bytes);
    }

    public void setMetaTopChunkSegmentIndexFromRepl(byte mergeWorkerId, byte batchIndex, byte slot, int nextIndex) {
        metaTopChunkSegmentIndex.put(mergeWorkerId, batchIndex, slot, nextIndex);
    }

    private int compressLevel;

    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    // decompress is cpu intensive
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

    public void initTopMergeSegmentIndexMap(File persistDir) throws IOException {
        this.metaTopChunkSegmentIndex = new MetaTopChunkSegmentIndex(slotNumber, topMergeWorkers,
                requestWorkers + mergeWorkers, persistDir);
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

        var toInt = ofInteger();
        this.intervalSeconds = chunkConfig.get(toInt, "merge.top.scheduleIntervalSeconds", 1);
        this.mergeTriggerPersistForcePerIntervalCount = 10 / intervalSeconds;
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

    private final int intervalSeconds;
    private final int mergeTriggerPersistForcePerIntervalCount;
    private long intervalCount = 0;

    private void mergeOne(int workerIndex, byte slot, byte batchIndex, int[] topMergeContinueSkipCountBySlot) {
        var logMerge = Debug.getInstance().logMerge;
        var topWorker = topChunkMergeWorkers[workerIndex];
        int segmentIndex = metaTopChunkSegmentIndex.get(topWorker.mergeWorkerId, batchIndex, slot);

        ArrayList<Integer> segmentIndexList = new ArrayList<>();
        segmentIndexList.add(segmentIndex);

        int k = workerIndex * slotNumber + slot;
        try {
            // wait until done
            int validCvCount = execute(topWorker.mergeWorkerId, slot, batchIndex, segmentIndexList).get();
            if (validCvCount == -1) {
                topMergeContinueSkipCountBySlot[k]++;
                // stay at current index
                if (topMergeContinueSkipCountBySlot[k] % 100 == 0 && slot == 0) {
                    log.info("Top self merge, continue skip count={}, w={}, s={}, b={}, i={}", topMergeContinueSkipCountBySlot[k],
                            topWorker.mergeWorkerId, slot, batchIndex, segmentIndex);
                }
            } else {
                topMergeContinueSkipCountBySlot[k] = 0;
                if (logMerge || (segmentIndex % 100 == 0 && slot == 0)) {
                    log.info("Top self merge, valid cv count={}, w={}, s={}, b={}, i={}",
                            validCvCount, topWorker.mergeWorkerId, slot, batchIndex, segmentIndex);
                }

                int nextIndex = segmentIndex == ConfForSlot.global.confChunk.maxSegmentIndex() ? 0 : segmentIndex + 1;
                metaTopChunkSegmentIndex.put(topWorker.mergeWorkerId, batchIndex, slot, nextIndex);

                var masterUpdateCallback = masterUpdateCallbackBySlot.get(slot);
                if (masterUpdateCallback != null) {
                    masterUpdateCallback.onTopMergeSegmentIndexUpdate(topWorker.mergeWorkerId, batchIndex, slot, nextIndex);
                }

                if (nextIndex == 0) {
                    log.info("Top self merge segment index go back to 0, w={}, s={}", topWorker.mergeWorkerId, slot);
                }
            }
        } catch (Exception e) {
            log.error("Execute top merge error", e);
        }
    }

    public void beginIntervalTopMerge() {
        this.mergeSelfScheduledExecutor = Executors.newScheduledThreadPool(1, topMergeWorkerThreadFactoryGroup2);

        int len = topChunkMergeWorkers.length * slotNumber;
        int[] topMergeContinueSkipCountBySlot = new int[len];

        var batchNumber = ConfForSlot.global.confWal.batchNumber;

        // for debug wait
        final int delaySeconds = 30;
        log.info("Begin interval top merge, delay seconds={}", delaySeconds);
        this.mergeSelfScheduledExecutor.scheduleWithFixedDelay(() -> {
            intervalCount++;
            for (int workerIndex = 0; workerIndex < topChunkMergeWorkers.length; workerIndex++) {
                for (int slot = 0; slot < slotNumber; slot++) {
                    for (int batchIndex = 0; batchIndex < batchNumber; batchIndex++) {
                        mergeOne(workerIndex, (byte) slot, (byte) batchIndex, topMergeContinueSkipCountBySlot);
                    }
                }
            }

            if (intervalCount % mergeTriggerPersistForcePerIntervalCount == 0) {
                var triggerCv = CompressedValue.createTriggerCv();

                for (int i = 0; i < slotNumber; i++) {
                    int chooseIndex = i % chunkMergeWorkers.length;
                    var mergeWorker = chunkMergeWorkers[chooseIndex];
                    mergeWorker.triggerPersistForce((byte) i, triggerCv);

                    if (i == 0 && intervalCount % 100 == 0) {
                        log.info("Trigger merge persist force, w={}, s={}", mergeWorker.mergeWorkerId, i);
                    }
                }
            }
        }, delaySeconds, intervalSeconds, java.util.concurrent.TimeUnit.SECONDS);
        log.info("Begin interval top merge, interval seconds={}", intervalSeconds);
    }

    public void start() {
        beginIntervalTopMerge();
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

        mergeSelfScheduledExecutor.shutdownNow();
        System.out.println("Shutdown top merge schedule worker");

        if (metaTopChunkSegmentIndex != null) {
            metaTopChunkSegmentIndex.cleanUp();
        }
    }

    Future<Integer> execute(byte workerId, byte slot, byte batchIndex, ArrayList<Integer> needMergeSegmentIndexList) {
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
            return mergeWorker.execute(job);
        } else {
            if (workerId >= requestWorkers + chunkMergeWorkers.length) {
                // this is already the top merge worker, use same file to merge, use lock to avoid conflict
                for (var topMergeWorker : topChunkMergeWorkers) {
                    if (topMergeWorker.mergeWorkerId == workerId) {
                        job.mergeWorker = topMergeWorker;
                        return topMergeWorker.execute(job);
                    }
                }
            } else {
                // by slot or by worker id?
                int chooseIndex = slot % topChunkMergeWorkers.length;
                var topMergeWorker = topChunkMergeWorkers[chooseIndex];
                job.mergeWorker = topMergeWorker;
                return topMergeWorker.execute(job);
            }
        }
        // should not reach here
        return CompletableFuture.completedFuture(job.validCvCountAfterRun);
    }

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();

        final String prefix = "chunk merge ";
        list.add(new StatKV(prefix + "worker number", chunkMergeWorkers.length));
        list.add(new StatKV(prefix + "top worker number", topChunkMergeWorkers.length));
        list.add(StatKV.split);

        for (var worker : chunkMergeWorkers) {
            list.addAll(worker.stats());
            list.add(StatKV.split);
        }
        for (var topWorker : topChunkMergeWorkers) {
            list.addAll(topWorker.stats());
            list.add(StatKV.split);
        }

        return list;
    }
}
