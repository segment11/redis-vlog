package redis.persist;

import io.activej.async.callback.AsyncComputation;
import io.activej.eventloop.Eventloop;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.ConfForSlot;
import redis.Debug;
import redis.ThreadFactoryAssignSupport;
import redis.metric.SimpleGauge;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static redis.persist.Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED;

public class ChunkMergeWorker {
    byte mergeWorkerId;
    private String mergeWorkerIdStr;
    private short slotNumber;
    private byte netWorkers;
    private byte mergeWorkers;
    private byte topMergeWorkers;

    private ChunkMerger chunkMerger;

    long mergedSegmentCount = 0;
    long mergedSegmentCostTotalTimeNanos = 0;
    byte lastMergedWorkerId = -1;
    byte lastMergedSlot = -1;
    int lastMergedSegmentIndex = -1;

    long validCvCountTotal = 0;
    long invalidCvCountTotal = 0;

    final Logger log = LoggerFactory.getLogger(getClass());

    private final LocalPersist localPersist = LocalPersist.getInstance();

    // index is batch index
    private Eventloop[] mergeHandleEventloopArray;

    boolean isTopMergeWorker;

    // just for config parameter
    int compressLevel;

    record CvWithKeyAndBucketIndex(CompressedValue cv, String key, int bucketIndex) {
    }

    private static final int MERGING_CV_SIZE_THRESHOLD = 1000;
    private static final int MERGED_SEGMENT_SET_SIZE_THRESHOLD = 100;

    private final List<CvWithKeyAndBucketIndex>[][] mergedCvListBySlotAndBatchIndex;

    void addMergedCv(byte slot, byte batchIndex, CvWithKeyAndBucketIndex cvWithKeyAndBucketIndex) {
        mergedCvListBySlotAndBatchIndex[slot][batchIndex].add(cvWithKeyAndBucketIndex);
    }

    boolean persistMergedCvList(byte slot, byte batchIndex) {
        var mergedCvList = mergedCvListBySlotAndBatchIndex[slot][batchIndex];
        var mergedSegmentSet = mergedSegmentSets[batchIndex];

        if (mergedCvList.size() < MERGING_CV_SIZE_THRESHOLD) {
            var mergedSegmentCount = mergedSegmentSet.stream().filter(one -> one.slot == slot).count();
            if (mergedSegmentCount < MERGED_SEGMENT_SET_SIZE_THRESHOLD) {
                return false;
            }
        }

        // persist to chunk
        var oneSlot = localPersist.oneSlot(slot);
        var targetChunk = oneSlot.chunksArray[mergeWorkerId][batchIndex];

        ArrayList<Integer> needMergeSegmentIndexListAll = new ArrayList<>();

        var groupByWalGroupIndex = mergedCvList.stream().collect(Collectors.groupingBy(one -> Wal.calWalGroupIndex(one.bucketIndex)));
        for (var entry : groupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var cvList = entry.getValue();

            ArrayList<Wal.V> list = new ArrayList<>();
            for (var cvWithKeyAndBucketIndex : cvList) {
                var cv = cvWithKeyAndBucketIndex.cv;
                var key = cvWithKeyAndBucketIndex.key;
                var bucketIndex = cvWithKeyAndBucketIndex.bucketIndex;

                list.add(new Wal.V(mergeWorkerId, cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                        key, cv.encode(), cv.compressedLength()));
            }

            // if list size is too large, need multi batch persist, todo
            var needMergeSegmentIndexList = targetChunk.persist(walGroupIndex, list, true);
            if (needMergeSegmentIndexList == null) {
                log.error("Merge worker persist merged cv list error, w={}, s={}", mergeWorkerId, slot);
                throw new RuntimeException("Merge worker persist merged cv list error, w=" + mergeWorkerId + ", s=" + slot);
            }

            needMergeSegmentIndexListAll.addAll(needMergeSegmentIndexList);
        }

        if (!mergedSegmentSet.isEmpty()) {
            var sb = new StringBuilder();
            var it = mergedSegmentSet.iterator();

            while (it.hasNext()) {
                var one = it.next();
                if (one.slot != slot) {
                    continue;
                }

                // can reuse this chunk by segment index
                oneSlot.setSegmentMergeFlag(one.workerId, one.batchIndex, one.index, SEGMENT_FLAG_MERGED_AND_PERSISTED, this.mergeWorkerId, 0L);
                it.remove();

                lastPersistedSegmentIndex = one.index;

                sb.append(one.workerId).append(",")
                        .append(one.batchIndex).append(",")
                        .append(one.index).append(";");
            }

            var doLog = (lastPersistedSegmentIndex % 500 == 0 && slot == 0) || Debug.getInstance().logMerge;
            if (doLog) {
                log.info("P s:{}, {}", slot, sb);
            }
        }

        if (!needMergeSegmentIndexListAll.isEmpty()) {
            chunkMerger.submit(mergeWorkerId, slot, batchIndex, needMergeSegmentIndexListAll);
        }

        lastPersistAtMillis = System.currentTimeMillis();
        return true;
    }

    long lastPersistAtMillis;

    int lastPersistedSegmentIndex;

    public record MergedSegment(byte workerId, byte slot, byte batchIndex, int index,
                                int validCvCount) implements Comparable<MergedSegment> {
        @Override
        public String toString() {
            return "MergedSegment{" +
                    ", slot=" + slot +
                    ", batchIndex=" + batchIndex +
                    ", index=" + index +
                    ", validCvCount=" + validCvCount +
                    '}';
        }

        @Override
        public int compareTo(@NotNull ChunkMergeWorker.MergedSegment o) {
            if (this.slot != o.slot) {
                return this.slot - o.slot;
            }
            if (this.batchIndex != o.batchIndex) {
                return this.batchIndex - o.batchIndex;
            }
            return this.index - o.index;
        }
    }

    // index is batch index
    final TreeSet<MergedSegment>[] mergedSegmentSets;

    public void initEventloop(boolean isTopMergeWorker) {
        this.isTopMergeWorker = isTopMergeWorker;

        var batchNumber = ConfForSlot.global.confWal.batchNumber;

        this.mergeHandleEventloopArray = new Eventloop[batchNumber];
        for (int batchIndex = 0; batchIndex < batchNumber; batchIndex++) {
            var mergeHandleEventloop = Eventloop.builder()
                    .withThreadName("chunk-merge-worker-batch-" + batchIndex)
                    .withIdleInterval(Duration.ofMillis(ConfForSlot.global.eventLoopIdleMillis))
                    .build();
            mergeHandleEventloop.keepAlive(true);
            this.mergeHandleEventloopArray[batchIndex] = mergeHandleEventloop;
        }
        log.info("Create chunk merge handle eventloop {}", mergeWorkerId);
    }

    public void fixMergeHandleChunkThreadId(Chunk chunk) {
        this.mergeHandleEventloopArray[chunk.batchIndex].submit(() -> {
            chunk.threadIdProtectedWhenWrite = Thread.currentThread().threadId();
            log.warn("Fix merge worker chunk thread id, w={}, mw={}, s={}, b={}, tid={}",
                    chunk.workerId, mergeWorkerId, chunk.slot, chunk.batchIndex, chunk.threadIdProtectedWhenWrite);
        });
    }

    public void submitWriteSegmentsFromMasterExists(Chunk chunk, byte[] bytes, int segmentIndex, int segmentCount, List<Long> segmentSeqList, int capacity) {
        this.mergeHandleEventloopArray[chunk.batchIndex].submit(() -> {
            chunk.writeSegmentsFromMasterExists(bytes, segmentIndex, segmentCount, segmentSeqList, capacity);
        });
    }

    public ChunkMergeWorker(byte mergeWorkerId, short slotNumber,
                            byte netWorkers, byte mergeWorkers, byte topMergeWorkers,
                            ChunkMerger chunkMerger) {
        this.mergeWorkerId = mergeWorkerId;
        this.mergeWorkerIdStr = String.valueOf(mergeWorkerId);
        this.slotNumber = slotNumber;
        this.netWorkers = netWorkers;
        this.mergeWorkers = mergeWorkers;
        this.topMergeWorkers = topMergeWorkers;
        this.chunkMerger = chunkMerger;

        var batchNumber = ConfForSlot.global.confWal.batchNumber;

        this.mergedCvListBySlotAndBatchIndex = new ArrayList[slotNumber][batchNumber];
        for (int i = 0; i < slotNumber; i++) {
            mergedCvListBySlotAndBatchIndex[i] = new ArrayList[batchNumber];
            for (int j = 0; j < batchNumber; j++) {
                mergedCvListBySlotAndBatchIndex[i][j] = new ArrayList<>(MERGING_CV_SIZE_THRESHOLD);
            }
        }

        this.mergedSegmentSets = new TreeSet[batchNumber];
        for (int i = 0; i < batchNumber; i++) {
            mergedSegmentSets[i] = new TreeSet<>();
        }

        this.initMetricsCollect();
    }

    void start() {
        for (int i = 0; i < mergeHandleEventloopArray.length; i++) {
            var mergeHandleEventloop = mergeHandleEventloopArray[i];
            var threadFactory = ThreadFactoryAssignSupport.getInstance().ForChunkMerge.getNextThreadFactory();
            var thread = threadFactory.newThread(mergeHandleEventloop);
            thread.start();
            log.info("Chunk merge handle eventloop thread started, w={}, b={}, thread name={}", mergeWorkerId, i, thread.getName());
        }
    }

    void stop() {
        for (var mergeHandleEventloop : mergeHandleEventloopArray) {
            mergeHandleEventloop.breakEventloop();
        }
        System.out.println("Stop chunk merge worker " + mergeWorkerId + " eventloop");
    }

    private static final SimpleGauge innerGauge = new SimpleGauge("chunk_merge_worker", "chunk merge worker",
            "merge_worker_id");

    static {
        innerGauge.register();
    }

    private void initMetricsCollect() {
        innerGauge.addRawGetter(() -> {
            var labelValues = List.of(mergeWorkerIdStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            if (mergedSegmentCount > 0) {
                map.put("merged_segment_count", new SimpleGauge.ValueWithLabelValues((double) mergedSegmentCount, labelValues));
                double mergedSegmentCostTAvg = (double) mergedSegmentCostTotalTimeNanos / mergedSegmentCount / 1000;
                map.put("merged_segment_cost_avg_micros", new SimpleGauge.ValueWithLabelValues(mergedSegmentCostTAvg, labelValues));

                map.put("valid_cv_count_total", new SimpleGauge.ValueWithLabelValues((double) validCvCountTotal, labelValues));
                map.put("invalid_cv_count_total", new SimpleGauge.ValueWithLabelValues((double) invalidCvCountTotal, labelValues));

                double validCvCountAvg = (double) validCvCountTotal / mergedSegmentCount;
                map.put("valid_cv_count_avg", new SimpleGauge.ValueWithLabelValues(validCvCountAvg, labelValues));

                double validCvRate = (double) validCvCountTotal / (validCvCountTotal + invalidCvCountTotal);
                map.put("valid_cv_rate", new SimpleGauge.ValueWithLabelValues(validCvRate, labelValues));
            }

            map.put("last_merged_worker_id", new SimpleGauge.ValueWithLabelValues((double) lastMergedWorkerId, labelValues));
            map.put("last_merged_slot", new SimpleGauge.ValueWithLabelValues((double) lastMergedSlot, labelValues));
            map.put("last_merged_segment_index", new SimpleGauge.ValueWithLabelValues((double) lastMergedSegmentIndex, labelValues));

            return map;
        });
    }

    CompletableFuture<Integer> submit(ChunkMergeJob job) {
        return mergeHandleEventloopArray[job.batchIndex].submit(AsyncComputation.of(job));
    }
}
