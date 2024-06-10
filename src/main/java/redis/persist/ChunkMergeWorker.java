package redis.persist;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.Debug;
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
    final OneSlot oneSlot;

    // metrics
    long mergedSegmentCount = 0;
    long mergedSegmentCostTimeTotalUs = 0;
    int lastMergedSegmentIndex = -1;

    long validCvCountTotal = 0;
    long invalidCvCountTotal = 0;

    private final Logger log = LoggerFactory.getLogger(getClass());

    // just for config parameter
    int compressLevel;

    record CvWithKeyAndBucketIndex(CompressedValue cv, String key, int bucketIndex) {
    }

    private static final int MERGING_CV_SIZE_THRESHOLD = 100;
    // for better latency, because group by wal group, if wal groups is too large, need multi batch persist
    static final int MERGED_SEGMENT_SET_SIZE_THRESHOLD = 8;

    private final List<CvWithKeyAndBucketIndex> mergedCvList = new ArrayList<>(MERGING_CV_SIZE_THRESHOLD);

    void addMergedCv(CvWithKeyAndBucketIndex cvWithKeyAndBucketIndex) {
        mergedCvList.add(cvWithKeyAndBucketIndex);
    }

    private record MergedSegment(int index, int validCvCount) implements Comparable<MergedSegment> {
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

    private final TreeSet<MergedSegment> mergedSegmentSet = new TreeSet<>();

    void addMergedSegment(int segmentIndex, int validCvCount) {
        mergedSegmentSet.add(new MergedSegment(segmentIndex, validCvCount));
    }

    boolean persistMergedCvList() {
        logMergeCount++;
        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;

        if (mergedCvList.size() < MERGING_CV_SIZE_THRESHOLD) {
            if (mergedSegmentSet.size() < MERGED_SEGMENT_SET_SIZE_THRESHOLD) {
                return false;
            }
        }

        // perf bad, need optimize, todo
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
                        key, cv.encode(), cv.compressedLength(), true));
            }

            // refer Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // list size is not large, need not multi batch persist
            oneSlot.chunk.persist(walGroupIndex, list, true);
        }

        if (!mergedSegmentSet.isEmpty()) {
            if (doLog) {
                log.info("Compare chunk merged segment index end last time, end last time i: {}, ready to merged and persisted last i: {}",
                        oneSlot.chunk.mergedSegmentIndexEndLastTime, mergedSegmentSet.getLast().index);
            }

            var sb = new StringBuilder();
            var it = mergedSegmentSet.iterator();

            while (it.hasNext()) {
                var one = it.next();
                // can reuse this chunk by segment index
                oneSlot.updateSegmentMergeFlag(one.index, SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);
                it.remove();
                sb.append(one.index).append(";");
            }

            if (doLog) {
                log.info("P s:{}, {}", slot, sb);
            }
        }

        mergedCvList.clear();
        return true;
    }

    long logMergeCount = 0;

    public ChunkMergeWorker(byte slot, OneSlot oneSlot) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.oneSlot = oneSlot;

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

            map.put("chunk_last_merged_segment_index", new SimpleGauge.ValueWithLabelValues((double) lastMergedSegmentIndex, labelValues));

            return map;
        });
    }
}
