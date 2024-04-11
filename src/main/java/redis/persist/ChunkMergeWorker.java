package redis.persist;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.*;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static redis.CompressedValue.KEY_HEADER_LENGTH;
import static redis.CompressedValue.VALUE_HEADER_LENGTH;
import static redis.persist.Chunk.MERGE_READ_SEGMENT_ONCE_COUNT;
import static redis.persist.Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED;
import static redis.persist.ChunkMerger.mergeWorkerThreadFactoryGroup1;
import static redis.persist.ChunkMerger.topMergeWorkerThreadFactoryGroup1;
import static redis.persist.LocalPersist.PAGE_SIZE;

public class ChunkMergeWorker implements OfStats {
    private static final int QUEUE_CAPACITY = 10;

    byte mergeWorkerId;
    short slotNumber;
    byte requestWorkers;
    byte mergeWorkers;
    byte topMergeWorkers;

    SnowFlake snowFlake;
    ChunkMerger chunkMerger;

    int queueCapacity = QUEUE_CAPACITY;

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
    private LinkedBlockingQueue<Runnable>[] queues;

    private ExecutorService[] executors;

    private boolean isTopMergeWorker;

    // just for config parameter
    int compressLevel;

    private record CvWithKey(CompressedValue cv, String key) {
    }

    private static final int MERGING_CV_SIZE_THRESHOLD = 1000;
    private static final int MERGED_SEGMENT_SET_SIZE_THRESHOLD = 100;

    private final List<CvWithKey>[][] mergedCvListBySlotAndBatchIndex;

    void addMergedCv(byte slot, byte batchIndex, CvWithKey cvWithKey) {
        mergedCvListBySlotAndBatchIndex[slot][batchIndex].add(cvWithKey);
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

        ArrayList<Wal.V> list = new ArrayList<>();
        for (var cvWithKey : mergedCvList) {
            var cv = cvWithKey.cv;
            var key = cvWithKey.key;

            var bucketIndex = localPersist.bucketIndex(cv.getKeyHash());
            list.add(new Wal.V(mergeWorkerId, cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(),
                    key, cv.encode(), cv.compressedLength()));

            if (list.size() >= MERGING_CV_SIZE_THRESHOLD) {
                var needMergeSegmentIndexList = targetChunk.persist(list);
                if (needMergeSegmentIndexList == null) {
                    log.error("Merge worker persist merged cv list error, w={}, s={}", mergeWorkerId, slot);
                    return false;
                }

                needMergeSegmentIndexListAll.addAll(needMergeSegmentIndexList);
                list.clear();
            }
        }
        if (!list.isEmpty()) {
            var needMergeSegmentIndexList = targetChunk.persist(list);
            if (needMergeSegmentIndexList == null) {
                log.error("Merge worker persist merged cv list error, w={}, s={}", mergeWorkerId, slot);
                return false;
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

        if (!isTopMergeWorker) {
            if (!needMergeSegmentIndexListAll.isEmpty()) {
                // need trigger top merge worker to merge
                chunkMerger.execute(mergeWorkerId, slot, batchIndex, needMergeSegmentIndexListAll);
            }
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
                    ", s=" + slot +
                    ", b=" + batchIndex +
                    ", i=" + index +
                    ", v=" + validCvCount +
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

    public void initExecutor(boolean isTopMergeWorker) {
        this.isTopMergeWorker = isTopMergeWorker;

        var batchNumber = ConfForSlot.global.confWal.batchNumber;

        this.queueCapacity = Math.max(QUEUE_CAPACITY, requestWorkers * slotNumber * 2);

        this.queues = new LinkedBlockingQueue[batchNumber];
        this.executors = new ThreadPoolExecutor[batchNumber];
        for (int i = 0; i < batchNumber; i++) {
            this.queues[i] = new LinkedBlockingQueue<>(this.queueCapacity);
            this.executors[i] = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    queues[i],
                    getThreadFactoryGroup(mergeWorkerId));
        }
        log.info("Create chunk merge worker executor {}", mergeWorkerId);
    }

    public void fixChunkThreadId(Chunk chunk) {
        this.executors[chunk.batchIndex].submit(() -> {
            chunk.threadIdProtected = Thread.currentThread().threadId();
            chunk.setWorkerType(false, true, isTopMergeWorker);
            log.warn("Fix merge worker chunk thread id, w={}, mw={}, s={}, b={}, tid={}",
                    chunk.workerId, mergeWorkerId, chunk.slot, chunk.batchIndex, chunk.threadIdProtected);
        });
    }

    public void submitWriteSegmentsMasterNewly(Chunk chunk, byte[] bytes, int segmentIndex, int segmentCount, ArrayList<Long> segmentSeqList, int capacity) {
        this.executors[chunk.batchIndex].submit(() -> {
            chunk.writeSegmentsFromMasterNewly(bytes, segmentIndex, segmentCount, segmentSeqList, capacity);
        });
    }

    private ThreadFactory getThreadFactoryGroup(byte mergeWorkerId) {
        // every cpu vCore has 10 threads
        // one core decompress usually 1000MB/s, 1 thread can handle 100MB/s
        // 4KB usually compress/decompress cost 10us
        // each 4KB includes 10+ compressed kv, 100MB = 25000 pages, qps is 25000+
        final int step = 10;
        int beginWorkerId = isTopMergeWorker ? requestWorkers + mergeWorkers : requestWorkers;
        int diff = mergeWorkerId - beginWorkerId;

        if (isTopMergeWorker) {
            if (diff < step) {
                return topMergeWorkerThreadFactoryGroup1;
            } else {
                return ChunkMerger.topMergeWorkerThreadFactoryGroup2;
            }
        }

        if (diff < step) {
            return mergeWorkerThreadFactoryGroup1;
        } else if (diff < step * 2) {
            return ChunkMerger.mergeWorkerThreadFactoryGroup2;
        } else if (diff < step * 3) {
            return ChunkMerger.mergeWorkerThreadFactoryGroup3;
        } else {
            return ChunkMerger.mergeWorkerThreadFactoryGroup4;
        }
    }

    public ChunkMergeWorker(byte mergeWorkerId, short slotNumber,
                            byte requestWorkers, byte mergeWorkers, byte topMergeWorkers,
                            SnowFlake snowFlake, ChunkMerger chunkMerger) {
        this.mergeWorkerId = mergeWorkerId;
        this.slotNumber = slotNumber;
        this.requestWorkers = requestWorkers;
        this.mergeWorkers = mergeWorkers;
        this.topMergeWorkers = topMergeWorkers;
        this.snowFlake = snowFlake;
        this.chunkMerger = chunkMerger;

        int batchNumber = ConfForSlot.global.confWal.batchNumber;

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
    }

    void start() {
        log.info("Start chunk merge worker {}", mergeWorkerId);
    }

    void stop() {
        for (var executor : executors) {
            executor.shutdownNow();
        }
        System.out.println("Stop chunk merge worker " + mergeWorkerId + " executor service");
    }

    void triggerPersistForce(byte slot, CompressedValue triggerCv) {
//        executor.submit(() -> {
//            var writeToChunk = toMergeChunks[slot];
//            WriteBatchKV.writeBatchKV.put(slot, mergeWorkerId, "", triggerCv, false, (slotInner, workerId, list, includeLast) -> {
//                writeToChunk.persist(list, includeLast);
//                WriteBatchKV.writeBatchKV.clearAfterPersist(slot, writeToChunk.lastPersistSegmentIndex(), workerId, includeLast);
//            });
//        });
    }

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();

        final String prefix = "chunk-merge-w-" + mergeWorkerId + " ";
        list.add(new StatKV(prefix + "merged segment count", mergedSegmentCount));

        if (mergedSegmentCount > 0) {
            list.add(new StatKV(prefix + "merged segment cost total millis", (double) mergedSegmentCostTotalTimeNanos / 1000 / 1000));
            double mergedSegmentCostTAvg = (double) mergedSegmentCostTotalTimeNanos / mergedSegmentCount / 1000;
            list.add(new StatKV(prefix + "merged segment cost avg micros", mergedSegmentCostTAvg));

            list.add(new StatKV(prefix + "valid cv count total", validCvCountTotal));
            list.add(new StatKV(prefix + "invalid cv count total", invalidCvCountTotal));
            double validCvCountAvg = (double) validCvCountTotal / mergedSegmentCount;
            list.add(new StatKV(prefix + "valid cv count avg", validCvCountAvg));

            double validCvRate = (double) validCvCountTotal / (validCvCountTotal + invalidCvCountTotal);
            // if valid cv rate is high > 0.5, need increase fdPerChunk in ConfForSlot
            list.add(new StatKV(prefix + "valid cv rate", validCvRate));
        }

        list.add(new StatKV(prefix + "last merged worker id", lastMergedWorkerId));
        list.add(new StatKV(prefix + "last merged slot", lastMergedSlot));
        list.add(new StatKV(prefix + "last merged segment index", lastMergedSegmentIndex));
        list.add(new StatKV(prefix + "batch index 0 wait queue size", queues[0].size()));

        list.add(StatKV.split);
        return list;
    }

    Future<Integer> execute(Job job) {
        try {
            var f = executors[job.batchIndex].submit(job);
            int queueSize = queues[job.batchIndex].size();
            if (queueSize >= queueCapacity / 2) {
                log.warn("!!!Chunk merge worker queue size is too large after submit, w={}, s={}, b={}, i={}, mw={}, queue size={}, queue capacity={}",
                        job.workerId, job.slot, job.batchIndex, job.needMergeSegmentIndexList, mergeWorkerId, queueSize, queueCapacity);
                // need change rate limit for request set, todo
            }
            return f;
        } catch (RejectedExecutionException e) {
            log.warn("Reject chunk merge job, w={}, s={}, b={}, i={}, mw={}", job.workerId, job.slot, job.batchIndex, job.needMergeSegmentIndexList, mergeWorkerId);
            throw e;
        }
    }

    static class Job implements Callable<Integer> {
        byte workerId;
        byte slot;
        byte batchIndex;
        ArrayList<Integer> needMergeSegmentIndexList;
        ChunkMergeWorker mergeWorker;
        SnowFlake snowFlake;
        int validCvCountAfterRun = 0;
        int invalidCvCountAfterRun = 0;

        private final LocalPersist localPersist = LocalPersist.getInstance();

        @Override
        public Integer call() {
            boolean isTopMergeWorkerSelfMerge = workerId == mergeWorker.mergeWorkerId;

            var batchCount = needMergeSegmentIndexList.size() / MERGE_READ_SEGMENT_ONCE_COUNT;
            if (needMergeSegmentIndexList.size() % MERGE_READ_SEGMENT_ONCE_COUNT != 0) {
                batchCount++;
            }
            try {
                for (int i = 0; i < batchCount; i++) {
                    var subList = needMergeSegmentIndexList.subList(i * MERGE_READ_SEGMENT_ONCE_COUNT,
                            Math.min((i + 1) * MERGE_READ_SEGMENT_ONCE_COUNT, needMergeSegmentIndexList.size()));
                    mergeSegments(isTopMergeWorkerSelfMerge, subList);
                }
                return validCvCountAfterRun;
            } catch (Exception e) {
                mergeWorker.log.error("Merge chunk error, w={}, s={}, b={}, i={}, mw={}, message={}", workerId, slot, batchIndex,
                        needMergeSegmentIndexList, mergeWorker.mergeWorkerId, e.getMessage());
                mergeWorker.log.error("Merge chunk error", e);
                return -1;
            }
        }

        private static class CvWithKeyAndSegmentOffset {
            public CvWithKeyAndSegmentOffset(CompressedValue cv, String key, int segmentOffset, int segmentIndex, byte subBlockIndex) {
                this.cv = cv;
                this.key = key;
                this.segmentOffset = segmentOffset;
                this.segmentIndex = segmentIndex;
                this.subBlockIndex = subBlockIndex;
            }

            CompressedValue cv;
            final String key;
            final long segmentOffset;
            final int segmentIndex;
            final byte subBlockIndex;
        }

        private static class ValidCvCountRecord {
            ValidCvCountRecord(int segmentIndex) {
                this.segmentIndex = segmentIndex;
            }

            int validCvCount;
            int invalidCvCount;
            final int segmentIndex;
        }

        private void mergeSegments(boolean isTopMergeWorkerSelfMerge, List<Integer> needMergeSegmentIndexList) {
            if (mergeWorker.isTopMergeWorker) {
                mergeWorker.log.debug("Add debug point here, w={}, s={}, mw={}", workerId, slot, mergeWorker.mergeWorkerId);
            }

            var firstSegmentIndex = needMergeSegmentIndexList.getFirst();
            var lastSegmentIndex = needMergeSegmentIndexList.getLast();
            assert needMergeSegmentIndexList.size() == lastSegmentIndex - firstSegmentIndex + 1;

            int segmentLength = ConfForSlot.global.confChunk.segmentLength;
            var npages0 = segmentLength / PAGE_SIZE;
            int npagesMerge = npages0 * MERGE_READ_SEGMENT_ONCE_COUNT;

            var oneSlot = localPersist.oneSlot(slot);

            HashSet<Integer> skipSegmentIndexSet = new HashSet<>();
            for (var segmentIndex : needMergeSegmentIndexList) {
                var segmentFlag = oneSlot.getSegmentMergeFlag(workerId, batchIndex, segmentIndex);
                // not write yet, skip
                if (segmentFlag == null || segmentFlag.flag() == Chunk.SEGMENT_FLAG_INIT) {
                    skipSegmentIndexSet.add(segmentIndex);
                    break;
                }

                var flag = segmentFlag.flag();
                // top merge worker force merge ignore flag
                if (flag == Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED && !isTopMergeWorkerSelfMerge) {
                    skipSegmentIndexSet.add(segmentIndex);
                    break;
                }

                if (flag == Chunk.SEGMENT_FLAG_MERGED) {
                    // if in merge chunk batch for very long time and not persisted yet, means valid cv count is too small
                    // need a persist trigger
                    // this chunk batch is already merged by other worker, skip
                    skipSegmentIndexSet.add(segmentIndex);
                    break;
                }

                // flag == Chunk.SEGMENT_FLAG_REUSE:
                // do nothing
                // write segment increase mush faster than merge segment (diff 16384+), merge executor already reject
                // will never happen here

                // flag == Chunk.SEGMENT_FLAG_MERGING:
                // do nothing
                // when server start, already recover segment flag
                // maybe crash before
                // continue to merge
            }

            var allSkipped = needMergeSegmentIndexList.size() == skipSegmentIndexSet.size();
            if (allSkipped) {
                return;
            }

            var chunk = oneSlot.chunksArray[mergeWorker.mergeWorkerId][batchIndex];
            // need synchronized read buffer？ not necessary, because merge worker executor is single thread, need check this
            var readForMergingBatchBuffer = isTopMergeWorkerSelfMerge ? chunk.readSelfForMergingBatchBuffer :
                    chunk.readForMergingBatchBuffer;

            mergeWorker.lastMergedWorkerId = workerId;
            mergeWorker.lastMergedSlot = slot;
            mergeWorker.lastMergedSegmentIndex = lastSegmentIndex;

            mergeWorker.mergedSegmentCount++;
            readForMergingBatchBuffer.clear();

            var beginT = System.nanoTime();
            int n = oneSlot.preadForMerge(workerId, batchIndex, firstSegmentIndex, readForMergingBatchBuffer, (long) firstSegmentIndex * segmentLength);
            if (n != npagesMerge * PAGE_SIZE) {
                throw new IllegalStateException("Read chunk error, w=" + workerId + ", s=" + slot +
                        ", i=" + firstSegmentIndex + ", n=" + n + ", segmentLength=" + segmentLength + ", npagesMerge=" + npagesMerge);
            }

            // only log slot 0, just for less log
            var doLog = (isTopMergeWorkerSelfMerge && firstSegmentIndex % 1000 == 0 && slot == 0) ||
                    (mergeWorker.isTopMergeWorker && !isTopMergeWorkerSelfMerge && firstSegmentIndex % 10000 == 0 && slot == 0) ||
                    (firstSegmentIndex % 10000 == 0 && slot == 0) ||
                    Debug.getInstance().logMerge;

            readForMergingBatchBuffer.rewind();
            // read all segments to memory
            ArrayList<CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(npagesMerge * 20);

            int i = 0;
            for (var segmentIndex : needMergeSegmentIndexList) {
                if (skipSegmentIndexSet.contains(segmentIndex)) {
                    // move to next segment
                    i++;
                    readForMergingBatchBuffer.position(i * segmentLength);
                    continue;
                }

                oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGING, mergeWorker.mergeWorkerId, snowFlake.nextId());
                if (doLog) {
                    mergeWorker.log.info("Set segment flag to merging, w={}, s={}, b={}, i={}, mw={}", workerId, slot, batchIndex, segmentIndex,
                            mergeWorker.mergeWorkerId);
                }

                // decompress
                var tightBytesLength = readForMergingBatchBuffer.getInt();
                // why? need check, todo
                if (tightBytesLength == 0) {
                    i++;
                    readForMergingBatchBuffer.position(i * segmentLength);
                    continue;
                }

                var tightBytesWithLength = new byte[4 + tightBytesLength];
                readForMergingBatchBuffer.position(readForMergingBatchBuffer.position() - 4).get(tightBytesWithLength);

                var buffer = ByteBuffer.wrap(tightBytesWithLength);
                // sub blocks
                // refer to SegmentBatch tight HEADER_LENGTH
                for (int j = 0; j < SegmentBatch.MAX_BLOCK_NUMBER; j++) {
                    buffer.position(4 + j * 4);
                    var subBlockOffset = buffer.getShort();
                    if (subBlockOffset == 0) {
                        break;
                    }

                    var subBlockLength = buffer.getShort();

                    var uncompressedBytes = new byte[segmentLength];
                    var d = Zstd.decompressByteArray(uncompressedBytes, 0, segmentLength,
                            tightBytesWithLength, subBlockOffset, subBlockLength);
                    if (d != segmentLength) {
                        throw new IllegalStateException("Decompress error, w=" + workerId + ", s=" + slot +
                                ", b=" + batchIndex + ", i=" + segmentIndex + ", sbi=" + j + ", d=" + d + ", segmentLength=" + segmentLength);
                    }

                    var buf = Unpooled.wrappedBuffer(uncompressedBytes);
                    buf.skipBytes(8 + 4 + 4);
                    // check segment crc, todo
//                long segmentSeq = buf.readLong();
//                int cvCount = buf.readInt();
//                int segmentMaskedValue = buf.readInt();

                    int offsetInThisSegment = Chunk.SEGMENT_HEADER_LENGTH;
                    while (true) {
                        if (buf.readableBytes() < 2) {
                            break;
                        }

                        var keyLength = buf.readByte();
                        if (keyLength == 0) {
                            break;
                        }

                        var keyBytes = new byte[keyLength];
                        buf.readBytes(keyBytes);
                        var key = new String(keyBytes);

                        var cv = CompressedValue.decode(buf, keyBytes, 0, false);

                        int offsetForThisCv = offsetInThisSegment;

                        int lenKey = KEY_HEADER_LENGTH + keyLength;
                        int lenValue = VALUE_HEADER_LENGTH + cv.compressedLength();
                        int length = lenKey + lenValue;

                        offsetInThisSegment += length;

                        cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetForThisCv, segmentIndex, (byte) j));
                    }
                }

                i++;
                // move to next segment
                readForMergingBatchBuffer.position(i * segmentLength);
            }

            ArrayList<ValidCvCountRecord> validCvCountRecordList = new ArrayList<>(needMergeSegmentIndexList.size());
            for (var segmentIndex : needMergeSegmentIndexList) {
                validCvCountRecordList.add(new ValidCvCountRecord(segmentIndex));
            }
            // compare to current wal or persisted value, remove deleted or old
            removeOld(oneSlot, cvList, validCvCountRecordList);

            for (var validCvCountRecord : validCvCountRecordList) {
                validCvCountAfterRun += validCvCountRecord.validCvCount;
                invalidCvCountAfterRun += validCvCountRecord.invalidCvCount;
            }

            HashSet<Integer> hasValidCvSegmentIndexSet = new HashSet<>();
            for (var one : cvList) {
                // use memory list, and threshold, then persist to merge worker's chunk
                mergeWorker.addMergedCv(slot, batchIndex, new CvWithKey(one.cv, one.key));
                hasValidCvSegmentIndexSet.add(one.segmentIndex);
            }

            boolean isPersisted = mergeWorker.persistMergedCvList(slot, batchIndex);
            if (isPersisted) {
                for (var segmentIndex : needMergeSegmentIndexList) {
                    oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, mergeWorker.mergeWorkerId, 0L);

                    if (doLog) {
                        var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                        mergeWorker.log.info("Set segment flag to persisted, w={}, s={}, b={}, i={}, mw={}, valid cv count={}, invalid cv count={}",
                                workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                    }
                }
            } else {
                for (var segmentIndex : needMergeSegmentIndexList) {
                    var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                    if (hasValidCvSegmentIndexSet.contains(segmentIndex)) {
                        oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGED, mergeWorker.mergeWorkerId, snowFlake.nextId());

                        if (doLog) {
                            mergeWorker.log.info("Set segment flag to merged, w={}, s={}, b={}, i={}, mw={}, valid cv count={}, invalid cv count={}",
                                    workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                        }

                        // add to merged set
                        mergeWorker.mergedSegmentSets[batchIndex].add(new MergedSegment(workerId, slot, batchIndex, segmentIndex, validCvCountRecord.validCvCount));
                    } else {
                        oneSlot.setSegmentMergeFlag(workerId, batchIndex, segmentIndex, Chunk.SEGMENT_FLAG_MERGED_AND_PERSISTED, mergeWorker.mergeWorkerId, 0L);

                        if (doLog) {
                            mergeWorker.log.info("Set segment flag to persisted, w={}, s={}, b={}, i={}, mw={}, valid cv count={}, invalid cv count={}",
                                    workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                        }
                    }
                }
            }

            var costT = System.nanoTime() - beginT;
            mergeWorker.mergedSegmentCostTotalTimeNanos += costT;

            mergeWorker.validCvCountTotal += validCvCountAfterRun;
            mergeWorker.invalidCvCountTotal += invalidCvCountAfterRun;
        }

        private void removeOld(OneSlot oneSlot, ArrayList<CvWithKeyAndSegmentOffset> cvList, ArrayList<ValidCvCountRecord> validCvCountRecordList) {
            var dictMap = DictMap.getInstance();
            var firstSegmentIndex = validCvCountRecordList.get(0).segmentIndex;
            ArrayList<CvWithKeyAndSegmentOffset> toRemoveCvList = new ArrayList<>(cvList.size());

            var groupByBucketIndex = cvList.stream().collect(Collectors.groupingBy(one -> localPersist.bucketIndex(one.cv.getKeyHash())));
            for (var entry : groupByBucketIndex.entrySet()) {
                var bucketIndex = entry.getKey();
                var keyBuckets = oneSlot.getKeyBuckets(bucketIndex);
                var splitNumber = keyBuckets.size();

                var list = entry.getValue();
                for (var one : list) {
                    var key = one.key;
                    var cv = one.cv;
                    var segmentOffset = one.segmentOffset;
                    var segmentIndex = one.segmentIndex;
                    var subBlockIndex = one.subBlockIndex;

                    var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);

                    byte[] valueBytesCurrent;
                    var tmpValueBytes = oneSlot.getFromWal(key, bucketIndex);
                    if (tmpValueBytes != null) {
                        // write batch kv is the newest
                        // need not thread safe, because newest seq must > cv seq
                        if (CompressedValue.isDeleted(tmpValueBytes)) {
                            validCvCountRecord.invalidCvCount++;
                            toRemoveCvList.add(one);
                            continue;
                        }
                        valueBytesCurrent = tmpValueBytes;
                    } else {
                        int splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(cv.getKeyHash() % splitNumber);
                        var keyBucket = keyBuckets.get(splitIndex);
                        var valueBytesWithExpireAt = keyBucket.getValueByKey(key.getBytes(), cv.getKeyHash());
                        if (valueBytesWithExpireAt != null && !valueBytesWithExpireAt.isExpired()) {
                            valueBytesCurrent = valueBytesWithExpireAt.valueBytes();
                        } else {
                            valueBytesCurrent = null;
                        }
                    }
                    if (valueBytesCurrent == null) {
                        validCvCountRecord.invalidCvCount++;
                        toRemoveCvList.add(one);
                        continue;
                    }

                    // if not meta
                    if (!PersistValueMeta.isPvm(valueBytesCurrent)) {
                        // compare seq
                        var buffer = ByteBuffer.wrap(valueBytesCurrent);

                        long valueSeqCurrent;
                        var firstByte = buffer.get(0);
                        // from write batch, maybe short value
                        if (firstByte < 0) {
                            valueSeqCurrent = buffer.position(1).getLong();
                        } else {
                            // normal compressed value encoded
                            valueSeqCurrent = buffer.getLong();
                        }

                        if (cv.getSeq() < valueSeqCurrent) {
                            // cv is old， discard
                            validCvCountRecord.invalidCvCount++;
                            toRemoveCvList.add(one);
                        } else {
                            // this else block will never happen
                            // because compressed value is newest from write batch kv
                            // from key loader, must be persisted value meta
                            throw new IllegalStateException("Merge compressed value is newer than write batch compressed value, w=" + workerId +
                                    ", s=" + slot + ", i=" + segmentIndex + ", key=" + key + ", merge cv seq=" + cv.getSeq() +
                                    ", write batch cv seq=" + valueSeqCurrent);
                        }
                    } else {
                        // compare is worker id, slot, offset is same
                        var pvmCurrent = PersistValueMeta.decode(valueBytesCurrent);
                        if (pvmCurrent.workerId != workerId || pvmCurrent.batchIndex != batchIndex
                                || pvmCurrent.segmentIndex != segmentIndex || pvmCurrent.subBlockIndex != subBlockIndex || pvmCurrent.segmentOffset != segmentOffset) {
                            // cv is old， discard
                            validCvCountRecord.invalidCvCount++;
                            toRemoveCvList.add(one);
                        } else {
                            if (cv.isExpired()) {
                                validCvCountRecord.invalidCvCount++;
                                toRemoveCvList.add(one);
                                // need not clear pvm, because key loader will clear
//                                oneSlot.removeDelay(mergeWorker.mergeWorkerId, key, bucketIndex, cv.getKeyHash(), cv.getSeq());

                                if (cv.isBigString()) {
                                    // need remove file
                                    var buffer = ByteBuffer.wrap(cv.getCompressedData());
                                    var uuid = buffer.getLong();

                                    var file = new File(oneSlot.getBigStringDir(), String.valueOf(uuid));
                                    if (!file.exists()) {
                                        mergeWorker.log.warn("Big string file not exists, w={}, s={}, b={}, i={}, mw={}, key={}, uuid={}",
                                                workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, key, uuid);
                                    } else {
                                        try {
                                            FileUtils.delete(file);
                                            mergeWorker.log.warn("Delete big string file, w={}, s={}, b={}, i={}, mw={}, key={}, uuid={}",
                                                    workerId, slot, batchIndex, segmentIndex, mergeWorker.mergeWorkerId, key, uuid);
                                        } catch (IOException e) {
                                            throw new RuntimeException("Delete big string file error, w=" + workerId + ", s=" + slot +
                                                    ", b=" + batchIndex + ", i=" + segmentIndex + ", mw=" + mergeWorker.mergeWorkerId +
                                                    ", key=" + key + ", uuid=" + uuid, e);
                                        }
                                    }
                                }
                            } else {
                                validCvCountRecord.validCvCount++;

                                // if there is a new dict, compress use new dict and replace
                                if (cv.isUseDict() && cv.getDictSeqOrSpType() == Dict.SELF_ZSTD_DICT_SEQ) {
                                    var dict = dictMap.getDict(TrainSampleJob.keyPrefix(key));
                                    if (dict != null) {
                                        var rawBytes = cv.decompress(Dict.SELF_ZSTD_DICT);
                                        var newCompressedCv = CompressedValue.compress(rawBytes, dict, mergeWorker.compressLevel);
                                        if (newCompressedCv.compressedLength() < cv.compressedLength()) {
                                            // replace
                                            newCompressedCv.setSeq(cv.getSeq());
                                            newCompressedCv.setDictSeqOrSpType(dict.getSeq());
                                            newCompressedCv.setKeyHash(cv.getKeyHash());
                                            newCompressedCv.setExpireAt(cv.getExpireAt());

                                            one.cv = newCompressedCv;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            cvList.removeAll(toRemoveCvList);
        }
    }
}
