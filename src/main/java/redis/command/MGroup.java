
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

public class MGroup extends BaseCommand {
    public MGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
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

    private Reply mget() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var replies = new Reply[data.length - 1];
        for (int i = 1; i < data.length; i++) {
            var keyBytes = data[i];
            var value = get(keyBytes);
            if (value == null) {
                replies[i] = NilReply.INSTANCE;
            } else {
                replies[i] = new BulkReply(value);
            }
        }

        return new MultiBulkReply(replies);
    }

    private Reply mset() {
        if (data.length < 3 || data.length % 2 == 0) {
            return ErrorReply.FORMAT;
        }

        // need slot lock ? todo
        for (int i = 1; i < data.length; i += 2) {
            var keyBytes = data[i];
            var valueBytes = data[i + 1];
            set(keyBytes, valueBytes);
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
                case "perfSkipPersist" -> Debug.getInstance().perfSkipPersist = isOn;
                case "perfSkipPvmUpdate" -> Debug.getInstance().perfSkipPvmUpdate = isOn;
                case "perfTestReadSegmentNoCache" -> Debug.getInstance().perfTestReadSegmentNoCache = isOn;
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

            var keyCount = bucketIndex == -1 ? oneSlot.getKeyCount() : oneSlot.getKeyCountInBucketIndex(bucketIndex);
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

            var keyBuckets = oneSlot.getKeyBuckets(bucketIndex);
            var sb = new StringBuilder();
            for (var one : keyBuckets) {
                sb.append(one).append("\n");
            }

            return new BulkReply(sb.toString().getBytes());
        }

        if (subCmd.equals("get-slot")) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            var keyBytes = data[2];
            var slotWithKeyHash = slot(keyBytes);
            return new BulkReply(slotWithKeyHash.toString().getBytes());
        }

        return NilReply.INSTANCE;
    }
}
