
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import org.apache.commons.io.FileUtils;
import redis.BaseCommand;
import redis.Debug;
import redis.reply.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MGroup extends BaseCommand {
    public MGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        if ("mget".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }

            ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
            for (int i = 1; i < data.length; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("mset".equals(cmd)) {
            if (data.length < 3 || data.length % 2 == 0) {
                return null;
            }

            ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
            for (int i = 1; i < data.length; i += 2) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        return null;
    }

    public Reply handle() {
        if ("mget".equals(cmd)) {
            return mget();
        }

        if ("mset".equals(cmd)) {
            return mset();
        }

        if ("manage".equals(cmd)) {
            return manage();
        }

        return NilReply.INSTANCE;
    }

    record KeyBytesAndSlotWithKeyHash(byte[] keyBytes, int index, SlotWithKeyHash slotWithKeyHash) {
    }

    record KeyValueBytesAndSlotWithKeyHash(byte[] keyBytes, byte[] valueBytes, SlotWithKeyHash slotWithKeyHash) {
    }

    record ValueBytesAndIndex(byte[] valueBytes, int index) {
    }

    private Reply mget() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            var replies = new Reply[data.length - 1];
            for (int i = 1; i < data.length; i++) {
                var keyBytes = data[i];
                var valueBytes = get(keyBytes);
                if (valueBytes == null) {
                    replies[i - 1] = NilReply.INSTANCE;
                } else {
                    replies[i - 1] = new BulkReply(valueBytes);
                }
            }
            return new MultiBulkReply(replies);
        }

        ArrayList<KeyBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1; i < data.length; i++) {
            var keyBytes = data[i];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
            list.add(new KeyBytesAndSlotWithKeyHash(keyBytes, i - 1, slotWithKeyHash));
        }

        ArrayList<CompletableFuture<ArrayList<ValueBytesAndIndex>>> futureList = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var f = oneSlot.threadSafeHandle(() -> {
                ArrayList<ValueBytesAndIndex> valueList = new ArrayList<>();
                for (var one : subList) {
                    var valueBytes = get(one.keyBytes, one.slotWithKeyHash);
                    valueList.add(new ValueBytesAndIndex(valueBytes, one.index));
                }
                return valueList;
            });
            futureList.add(f);
        }

        ArrayList<ValueBytesAndIndex> valueList = new ArrayList<>();
        var allFutures = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).whenComplete((v, e) -> {
            for (var f : futureList) {
                synchronized (valueList) {
                    valueList.addAll(f.getNow(null));
                }
            }
        });
        try {
            allFutures.get();
        } catch (Exception e) {
            log.error("mget error", e);
            return new ErrorReply(e.getMessage());
        }

        var replies = new Reply[data.length - 1];
        for (var one : valueList) {
            var index = one.index();
            var valueBytes = one.valueBytes();
            if (valueBytes == null) {
                replies[index] = NilReply.INSTANCE;
            } else {
                replies[index] = new BulkReply(valueBytes);
            }
        }
        return new MultiBulkReply(replies);
    }

    private Reply mset() {
        if (data.length < 3 || data.length % 2 == 0) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            for (int i = 1; i < data.length; i += 2) {
                var keyBytes = data[i];
                var valueBytes = data[i + 1];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(i);
                set(keyBytes, valueBytes, slotWithKeyHash);
            }
            return OKReply.INSTANCE;
        }

        ArrayList<KeyValueBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1; i < data.length; i += 2) {
            var keyBytes = data[i];
            var valueBytes = data[i + 1];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
            list.add(new KeyValueBytesAndSlotWithKeyHash(keyBytes, valueBytes, slotWithKeyHash));
        }

        ArrayList<CompletableFuture<Boolean>> futureList = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var f = oneSlot.threadSafeHandle(() -> {
                for (var one : subList) {
                    set(one.keyBytes, one.valueBytes, one.slotWithKeyHash);
                }
                return true;
            });
            futureList.add(f);
        }

        var allFutures = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        try {
            allFutures.get();
        } catch (Exception e) {
            log.error("mset error", e);
            return new ErrorReply(e.getMessage());
        }

        return OKReply.INSTANCE;
    }

    private Reply manage() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        String subCmd = new String(data[1]);

        if (subCmd.equals("debug")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var field = new String(data[2]);
            var val = new String(data[3]);
            var isOn = val.equals("1") || val.equals("true");

            switch (field) {
                case "logMerge" -> Debug.getInstance().logMerge = isOn;
                case "logCompress" -> Debug.getInstance().logCompress = isOn;
                case "logPersist" -> Debug.getInstance().logPersist = isOn;
                case "logRestore" -> Debug.getInstance().logRestore = isOn;
                case "perfSkipPvmUpdate" -> Debug.getInstance().perfSkipPvmUpdate = isOn;
                default -> {
                    return ErrorReply.FORMAT;
                }
            }

            return OKReply.INSTANCE;
        }

        if (subCmd.equals("view-segment-flag")) {
            if (data.length != 6) {
                return ErrorReply.FORMAT;
            }

            byte workerIdGiven = Byte.parseByte(new String(data[2]));
            byte slotGiven = Byte.parseByte(new String(data[3]));
            int indexGiven = Integer.parseInt(new String(data[4]));
            int segmentLength = Integer.parseInt(new String(data[5]));

            // todo
//            var segmentFlag = localPersist.getSegmentMergeFlag(segmentLength, workerIdGiven, slotGiven, indexGiven);
//            if (segmentFlag == null) {
//                return NilReply.INSTANCE;
//            }
//
//            var str = segmentFlag.flag() + "," + segmentFlag.workerId() + "," + segmentFlag.cvCount() + "," + segmentFlag.seq();
//            return new BulkReply(str.getBytes());
        }

        if (subCmd.equals("set-dict-key-prefix-groups")) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            var keyPrefixGroups = new String(data[2]);
            if (keyPrefixGroups.isEmpty()) {
                return ErrorReply.FORMAT;
            }

            ArrayList<String> keyPrefixGroupList = new ArrayList<>();
            for (var keyPrefixGroup : keyPrefixGroups.split(",")) {
                if (keyPrefixGroup.isEmpty()) {
                    return ErrorReply.FORMAT;
                }
                keyPrefixGroupList.add(keyPrefixGroup);
            }

            trainSampleJob.setKeyPrefixGroupList(keyPrefixGroupList);
            log.warn("Set dict key prefix groups: {}", keyPrefixGroups);
            return OKReply.INSTANCE;
        }

        if (subCmd.equals("view-bucket-key-count")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var slotBytes = data[2];
            var bucketIndexBytes = data[3];

            byte slot;
            int bucketIndex;

            try {
                slot = Byte.parseByte(new String(slotBytes));
                bucketIndex = Integer.parseInt(new String(bucketIndexBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.SYNTAX;
            }

            var oneSlot = localPersist.oneSlot(slot);

            var keyCount = bucketIndex == -1 ? oneSlot.getKeyCount() : oneSlot.getKeyLoader().getKeyCountInBucketIndex(bucketIndex);
            return new IntegerReply(keyCount);
        }

        if (subCmd.equals("trigger-merge-segment")) {
            if (data.length != 6) {
                return ErrorReply.FORMAT;
            }

            var workerIdBytes = data[2];
            var slotBytes = data[3];
            var batchIndexBytes = data[4];
            var segmentIndexBytes = data[5];

            byte workerId;
            byte slot;
            byte batchIndex;
            short segmentIndex;

            try {
                workerId = Byte.parseByte(new String(workerIdBytes));
                slot = Byte.parseByte(new String(slotBytes));
                batchIndex = Byte.parseByte(new String(batchIndexBytes));
                segmentIndex = Short.parseShort(new String(segmentIndexBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.SYNTAX;
            }

            try {
                int i = localPersist.startChunkMergerJob(workerId, slot, batchIndex, segmentIndex);
                return new IntegerReply(i);
            } catch (Exception e) {
                return new ErrorReply(e.getMessage());
            }
        }

        if (subCmd.equals("output-dict-bytes")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var dictSeqBytes = data[2];
            var keyBytes = data[3];
            var key = new String(keyBytes);
            int dictSeq;

            try {
                dictSeq = Integer.parseInt(new String(dictSeqBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.SYNTAX;
            }

            var dict = dictMap.getDictBySeq(dictSeq);
            var file = new File("/home/kerry/redis-d200-dict.txt");
            try {
                FileUtils.writeByteArrayToFile(file, dict.getDictBytes());
                log.info("Output dict bytes to file: {}", file.getAbsolutePath());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }

            var valueBytes = get(keyBytes);
            if (valueBytes == null) {
                return NilReply.INSTANCE;
            }

            var file2 = new File("/home/kerry/redis-d200-value.txt");
            try {
                FileUtils.writeByteArrayToFile(file2, valueBytes);
                log.info("Output value bytes to file: {}", file2.getAbsolutePath());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }

            return OKReply.INSTANCE;
        }

        if (subCmd.equals("view-merging-segment")) {
            if (data.length != 6) {
                return ErrorReply.FORMAT;
            }

            var mergeWorkerIdBytes = data[2];
            var workerIdBytes = data[3];
            var slotBytes = data[4];

            byte mergeWorkerId;
            byte workerId;
            byte slot;

            try {
                mergeWorkerId = Byte.parseByte(new String(mergeWorkerIdBytes));
                workerId = Byte.parseByte(new String(workerIdBytes));
                slot = Byte.parseByte(new String(slotBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.SYNTAX;
            }

            var oneSlot = localPersist.oneSlot(slot);

            var treeSet = oneSlot.getMergedSegmentSet(mergeWorkerId, workerId);
            var lastPersistAtMillis = oneSlot.getMergeLastPersistAtMillis(mergeWorkerId);
            var lastPersistedSegmentIndex = oneSlot.getMergeLastPersistedSegmentIndex(mergeWorkerId);

            var sb = new StringBuilder();

            var sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sb.append("merge last persist at: ").append(sf.format(new Date(lastPersistAtMillis))).append("\n");
            sb.append("merge last persisted segment index: ").append(lastPersistedSegmentIndex).append("\n");
            sb.append("merging segments: ").append(treeSet.size()).append("\n");
            for (var one : treeSet) {
                sb.append(one).append("\n");
            }

            return new BulkReply(sb.toString().getBytes());
        }

        if (subCmd.equals("view-slot-bucket-keys")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var slotBytes = data[2];
            var bucketIndexBytes = data[3];

            byte slot;
            int bucketIndex;

            try {
                slot = Byte.parseByte(new String(slotBytes));
                bucketIndex = Integer.parseInt(new String(bucketIndexBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.SYNTAX;
            }

            var oneSlot = localPersist.oneSlot(slot);

            var str = oneSlot.getKeyLoader().readKeyBucketsToStringForDebug(bucketIndex);
            return new BulkReply(str.getBytes());
        }

        if (subCmd.equals("get-slot-with-key-hash")) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            var keyBytes = data[2];
            var slotWithKeyHash = slot(keyBytes);
            return new BulkReply(slotWithKeyHash.toString().getBytes());
        }

        if (subCmd.equals("dyn-config")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var configKeyBytes = data[2];
            var configValueBytes = data[3];

            var configKey = new String(configKeyBytes);

            var oneSlots = localPersist.oneSlots();
            for (var oneSlot : oneSlots) {
                var f = oneSlot.threadSafeHandle(() -> oneSlot.updateDynConfig(configKey, configValueBytes));
                try {
                    var r = f.get();
                    if (!r) {
                        return new ErrorReply("update dyn config failed, slot: " + oneSlot.slot());
                    }
                } catch (Exception e) {
                    return new ErrorReply("update dyn config failed, slot: " + oneSlot.slot() + ", message: " + e.getMessage());
                }
            }
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }
}
