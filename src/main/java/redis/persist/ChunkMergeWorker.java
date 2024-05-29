package redis.persist;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.Debug;
import redis.SnowFlake;
import redis.metric.SimpleGauge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static redis.persist.Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED;

public class ChunkMergeWorker {
    private final byte slot;
    private final String slotStr;
    private final SnowFlake snowFlake;

    long mergedSegmentCount = 0;
    long mergedSegmentCostTimeTotalUs = 0;
    byte lastMergedWorkerId = -1;
    byte lastMergedSlot = -1;
    int lastMergedSegmentIndex = -1;

    long validCvCountTotal = 0;
    long invalidCvCountTotal = 0;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final LocalPersist localPersist = LocalPersist.getInstance();

    // just for config parameter
    int compressLevel;

    record CvWithKeyAndBucketIndex(CompressedValue cv, String key, int bucketIndex) {
    }

    private static final int MERGING_CV_SIZE_THRESHOLD = 1000;
    private static final int MERGED_SEGMENT_SET_SIZE_THRESHOLD = 100;

    private final List<CvWithKeyAndBucketIndex> mergedCvList = new ArrayList<>(MERGING_CV_SIZE_THRESHOLD);

    void addMergedCv(CvWithKeyAndBucketIndex cvWithKeyAndBucketIndex) {
        mergedCvList.add(cvWithKeyAndBucketIndex);
    }

    boolean persistMergedCvList() {
        if (mergedCvList.size() < MERGING_CV_SIZE_THRESHOLD) {
            if (mergedSegmentSet.size() < MERGED_SEGMENT_SET_SIZE_THRESHOLD) {
                return false;
            }
        }

        // persist to chunk
        var oneSlot = localPersist.oneSlot(slot);
        var chunk = oneSlot.chunk;

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

                list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                        key, cv.encode(), cv.compressedLength()));
            }

            // if list size is too large, need multi batch persist, todo
            var needMergeSegmentIndexList = chunk.persist(walGroupIndex, list, true);
            if (needMergeSegmentIndexList == null) {
                log.error("Merge worker persist merged cv list error, w={}, s={}", slot);
                throw new RuntimeException("Merge worker persist merged cv list error, s=" + slot);
            }

            needMergeSegmentIndexListAll.addAll(needMergeSegmentIndexList);
        }

        if (!mergedSegmentSet.isEmpty()) {
            var sb = new StringBuilder();
            var it = mergedSegmentSet.iterator();

            while (it.hasNext()) {
                var one = it.next();
                // can reuse this chunk by segment index
                oneSlot.setSegmentMergeFlag(one.index, SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);
                it.remove();

                lastPersistedSegmentIndex = one.index;

                sb.append(one.index).append(";");
            }

            var doLog = (lastPersistedSegmentIndex % 500 == 0 && slot == 0) || Debug.getInstance().logMerge;
            if (doLog) {
                log.info("P s:{}, {}", slot, sb);
            }
        }

        if (!needMergeSegmentIndexListAll.isEmpty()) {
            oneSlot.doMergeJob(needMergeSegmentIndexListAll);
        }

        lastPersistAtMillis = System.currentTimeMillis();
        return true;
    }

    long lastPersistAtMillis;

    int lastPersistedSegmentIndex;

    public record MergedSegment(int index, int validCvCount) implements Comparable<MergedSegment> {
        @Override
        public String toString() {
            return "MergedSegment{" +
                    ", index=" + index +
                    ", validCvCount=" + validCvCount +
                    '}';
        }

        @Override
        public int compareTo(@NotNull ChunkMergeWorker.MergedSegment o) {
            return this.index - o.index;
        }
    }

    final TreeSet<MergedSegment> mergedSegmentSet = new TreeSet<>();

    public ChunkMergeWorker(byte slot, SnowFlake snowFlake) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.snowFlake = snowFlake;

        this.initMetricsCollect();
    }

    private static final SimpleGauge innerGauge = new SimpleGauge("chunk_merge_worker", "chunk merge worker",
            "slot");

    static {
        innerGauge.register();
    }

    private void initMetricsCollect() {
        innerGauge.addRawGetter(() -> {
            var labelValues = List.of(slotStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            if (mergedSegmentCount > 0) {
                map.put("merged_segment_count", new SimpleGauge.ValueWithLabelValues((double) mergedSegmentCount, labelValues));
                double mergedSegmentCostTAvg = (double) mergedSegmentCostTimeTotalUs / mergedSegmentCount;
                map.put("merged_segment_cost_time_avg_us", new SimpleGauge.ValueWithLabelValues(mergedSegmentCostTAvg, labelValues));

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
}
