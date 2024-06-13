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

    record CvWithKeyAndBucketIndexAndSegmentIndex(CompressedValue cv, String key, int bucketIndex, int segmentIndex) {
    }

    // for better latency, because group by wal group, if wal groups is too large, need multi batch persist
    private int MERGED_SEGMENT_SIZE_THRESHOLD = 256;
    private int MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST = 8;
    private int MERGED_CV_SIZE_THRESHOLD = 256 * 64;

    void resetThreshold(int walGroupNumber) {
        MERGED_SEGMENT_SIZE_THRESHOLD = Math.min(walGroupNumber, 256);
        MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST = Math.max(MERGED_SEGMENT_SIZE_THRESHOLD / 32, 4);
        MERGED_CV_SIZE_THRESHOLD = MERGED_SEGMENT_SIZE_THRESHOLD * 64;

        log.info("Reset chunk merge worker threshold, wal group number: {}, merged segment size threshold: {}, " +
                        "merged segment size threshold once persist: {}, merged cv size threshold: {}",
                walGroupNumber, MERGED_SEGMENT_SIZE_THRESHOLD, MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST, MERGED_CV_SIZE_THRESHOLD);

        final var maybeOneMergedCvBytesLength = 200;
        var lruMemoryRequireMB = MERGED_CV_SIZE_THRESHOLD * maybeOneMergedCvBytesLength / 1024 / 1024;
        log.info("LRU max size for chunk segment merged cv buffer: {}, maybe one merged cv bytes length is {}B, memory require: {}MB, slot: {}",
                MERGED_CV_SIZE_THRESHOLD,
                maybeOneMergedCvBytesLength,
                lruMemoryRequireMB,
                slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.chunk_segment_merged_cv_buffer, lruMemoryRequireMB, false);
    }

    final List<CvWithKeyAndBucketIndexAndSegmentIndex> mergedCvList = new ArrayList<>(MERGED_CV_SIZE_THRESHOLD);

    void addMergedCv(CvWithKeyAndBucketIndexAndSegmentIndex cvWithKeyAndBucketIndexAndSegmentIndex) {
        mergedCvList.add(cvWithKeyAndBucketIndexAndSegmentIndex);
    }

    record MergedSegment(int segmentIndex, int validCvCount) implements Comparable<MergedSegment> {
        @Override
        public String toString() {
            return "S{" +
                    ", s=" + segmentIndex +
                    ", count=" + validCvCount +
                    '}';
        }

        @Override
        public int compareTo(@NotNull ChunkMergeWorker.MergedSegment o) {
            return this.segmentIndex - o.segmentIndex;
        }
    }

    final TreeSet<MergedSegment> mergedSegmentSet = new TreeSet<>();

    void addMergedSegment(int segmentIndex, int validCvCount) {
        mergedSegmentSet.add(new MergedSegment(segmentIndex, validCvCount));
    }

    OneSlot.BeforePersistWalExt2FromMerge getMergedButNotPersistedBeforePersistWal(int walGroupIndex) {
        if (mergedCvList.isEmpty()) {
            return null;
        }

        ArrayList<Integer> segmentIndexList = new ArrayList<>();

        ArrayList<Wal.V> vList = new ArrayList<>();
        for (var one : mergedCvList) {
            var cv = one.cv;
            var key = one.key;
            var bucketIndex = one.bucketIndex;

            var calWalGroupIndex = Wal.calWalGroupIndex(bucketIndex);
            if (calWalGroupIndex != walGroupIndex) {
                continue;
            }

            vList.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cv.encode(), cv.encodedLength(), true));
            if (!segmentIndexList.contains(one.segmentIndex)) {
                segmentIndexList.add(one.segmentIndex);
            }
        }

        if (vList.isEmpty()) {
            return null;
        }

        return new OneSlot.BeforePersistWalExt2FromMerge(segmentIndexList, vList);
    }

    void removeMergedButNotPersistedAfterPersistWal(ArrayList<Integer> segmentIndexList, int walGroupIndex) {
        mergedCvList.removeIf(one -> Wal.calWalGroupIndex(one.bucketIndex) == walGroupIndex);
        mergedSegmentSet.removeIf(one -> segmentIndexList.contains(one.segmentIndex));

        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;
        if (doLog) {
            log.info("After remove merged but not persisted, merged segment set: {}, merged cv list size: {}", mergedSegmentSet, mergedCvList.size());
        }
    }

    void persistFIFOMergedCvList() {
        logMergeCount++;
        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;

        // once only persist firstly merged segments
        ArrayList<Integer> oncePersistSegmentIndexList = new ArrayList<>(MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST);
        // already sorted by segmentIndex
        for (var mergedSegment : mergedSegmentSet) {
            if (oncePersistSegmentIndexList.size() >= MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST) {
                break;
            }

            oncePersistSegmentIndexList.add(mergedSegment.segmentIndex);
        }

        var groupByWalGroupIndex = mergedCvList.stream()
                .filter(one -> oncePersistSegmentIndexList.contains(one.segmentIndex))
                .collect(Collectors.groupingBy(one -> Wal.calWalGroupIndex(one.bucketIndex)));

        if (groupByWalGroupIndex.size() > MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST) {
            log.warn("Go to persist merged cv list once, perf bad, group by wal group index size: {}", groupByWalGroupIndex.size());
        }
        for (var entry : groupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var subMergedCvList = entry.getValue();

            ArrayList<Wal.V> list = new ArrayList<>();
            for (var one : subMergedCvList) {
                var cv = one.cv;
                var key = one.key;
                var bucketIndex = one.bucketIndex;

                list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                        key, cv.encode(), cv.encodedLength(), true));
            }

            // refer Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // list size is not large, need not multi batch persist
            oneSlot.chunk.persist(walGroupIndex, list, true);
        }

        if (doLog) {
            log.info("Compare chunk merged segment index end last time, end last time i: {}, ready to merged and persisted last i: {}",
                    oneSlot.chunk.mergedSegmentIndexEndLastTime, oncePersistSegmentIndexList.getLast());
        }

        var sb = new StringBuilder();
        var it = mergedSegmentSet.iterator();

        while (it.hasNext()) {
            var one = it.next();
            if (!oncePersistSegmentIndexList.contains(one.segmentIndex)) {
                continue;
            }

            // can reuse this chunk by segment segmentIndex
            oneSlot.updateSegmentMergeFlag(one.segmentIndex, SEGMENT_FLAG_MERGED_AND_PERSISTED, 0L);
            it.remove();
            sb.append(one.segmentIndex).append(";");
        }

        if (doLog) {
            log.info("P s:{}, {}", slot, sb);
        }

        mergedCvList.removeIf(one -> oncePersistSegmentIndexList.contains(one.segmentIndex));
    }

    boolean persistFIFOMergedCvListIfBatchSizeOk() {
        if (mergedCvList.size() < MERGED_CV_SIZE_THRESHOLD) {
            if (mergedSegmentSet.size() < MERGED_SEGMENT_SIZE_THRESHOLD) {
                return false;
            }
        }

        persistFIFOMergedCvList();
        return true;
    }

    long logMergeCount = 0;

    public ChunkMergeWorker(byte slot, OneSlot oneSlot) {
        this.slot = slot;
        this.slotStr = String.valueOf(slot);
        this.oneSlot = oneSlot;

        this.initMetricsCollect();
    }

    private final static SimpleGauge innerGauge = new SimpleGauge("chunk_merge_worker", "chunk merge worker",
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
            map.put("chunk_merged_but_not_persisted_segment_count", new SimpleGauge.ValueWithLabelValues((double) mergedSegmentSet.size(), labelValues));
            map.put("chunk_merged_but_not_persisted_cv_count", new SimpleGauge.ValueWithLabelValues((double) mergedCvList.size(), labelValues));

            return map;
        });
    }
}
