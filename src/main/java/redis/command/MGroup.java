
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.apache.commons.io.FileUtils;
import redis.BaseCommand;
import redis.Debug;
import redis.reply.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class MGroup extends BaseCommand {
    public MGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("mget".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }

            for (int i = 1; i < data.length; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("mset".equals(cmd)) {
            if (data.length < 3 || data.length % 2 == 0) {
                return slotWithKeyHashList;
            }

            for (int i = 1; i < data.length; i += 2) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("manage".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }

            var subCmd = new String(data[1]);
            if (subCmd.equals("view-persist-key-count") || subCmd.equals("view-slot-bucket-keys")) {
                if (data.length != 4) {
                    return slotWithKeyHashList;
                }

                var slotBytes = data[2];
                byte slot;
                try {
                    slot = Byte.parseByte(new String(slotBytes));
                } catch (NumberFormatException e) {
                    return slotWithKeyHashList;
                }

                slotWithKeyHashList.add(new SlotWithKeyHash(slot, 0, 0L));
                return slotWithKeyHashList;
            }

            if (subCmd.equals("output-dict-bytes")) {
                if (data.length != 4) {
                    return slotWithKeyHashList;
                }

                var keyBytes = data[3];
                slotWithKeyHashList.add(slot(keyBytes, slotNumber));
                return slotWithKeyHashList;
            }
        }

        return slotWithKeyHashList;
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

    Reply mget() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            var replies = new Reply[data.length - 1];
            for (int i = 1, j = 0; i < data.length; i++, j++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                var valueBytes = get(keyBytes, slotWithKeyHash);
                if (valueBytes == null) {
                    replies[j] = NilReply.INSTANCE;
                } else {
                    replies[j] = new BulkReply(valueBytes);
                }
            }
            return new MultiBulkReply(replies);
        }

        ArrayList<KeyBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var keyBytes = data[i];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new KeyBytesAndSlotWithKeyHash(keyBytes, j, slotWithKeyHash));
        }

        ArrayList<Promise<ArrayList<ValueBytesAndIndex>>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ArrayList<ValueBytesAndIndex> valueList = new ArrayList<>();
                for (var one : subList) {
                    var valueBytes = get(one.keyBytes, one.slotWithKeyHash);
                    valueList.add(new ValueBytesAndIndex(valueBytes, one.index));
                }
                return valueList;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("mget error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            ArrayList<ValueBytesAndIndex> valueList = new ArrayList<>();
            for (var p : promises) {
                valueList.addAll(p.getResult());
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
            finalPromise.set(new MultiBulkReply(replies));
        });

        return asyncReply;
    }

    Reply mset() {
        if (data.length < 3 || data.length % 2 == 0) {
            return ErrorReply.FORMAT;
        }

        if (!isCrossRequestWorker) {
            for (int i = 1, j = 0; i < data.length; i += 2, j++) {
                var keyBytes = data[i];
                var valueBytes = data[i + 1];
                var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
                set(keyBytes, valueBytes, slotWithKeyHash);
            }
            return OKReply.INSTANCE;
        }

        ArrayList<KeyValueBytesAndSlotWithKeyHash> list = new ArrayList<>();
        for (int i = 1, j = 0; i < data.length; i += 2, j++) {
            var keyBytes = data[i];
            var valueBytes = data[i + 1];
            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new KeyValueBytesAndSlotWithKeyHash(keyBytes, valueBytes, slotWithKeyHash));
        }

        ArrayList<Promise<Void>> promises = new ArrayList<>();
        var groupBySlot = list.stream().collect(Collectors.groupingBy(one -> one.slotWithKeyHash.slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncRun(() -> {
                for (var one : subList) {
                    set(one.keyBytes, one.valueBytes, one.slotWithKeyHash);
                }
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("mset error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            finalPromise.set(OKReply.INSTANCE);
        });

        return asyncReply;
    }

    private Reply manage() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]);

        if (subCmd.equals("debug")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var field = new String(data[2]);
            var val = new String(data[3]);
            var isOn = val.equals("1") || val.equals("true");

            switch (field) {
                case "logMerge" -> Debug.getInstance().logMerge = isOn;
                case "logTrainDict" -> Debug.getInstance().logTrainDict = isOn;
                case "logRestore" -> Debug.getInstance().logRestore = isOn;
                case "bulkLoad" -> Debug.getInstance().bulkLoad = isOn;
                default -> {
                    return ErrorReply.FORMAT;
                }
            }

            return OKReply.INSTANCE;
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

        if (subCmd.equals("view-persist-key-count")) {
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

            var keyCount = bucketIndex == -1 ? oneSlot.getAllKeyCount() : oneSlot.getKeyLoader().getKeyCountInBucketIndex(bucketIndex);
            return new IntegerReply(keyCount);
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

        if (subCmd.equals("output-dict-bytes")) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            var dictSeqBytes = data[2];
            int dictSeq;

            try {
                dictSeq = Integer.parseInt(new String(dictSeqBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.SYNTAX;
            }

            var dict = dictMap.getDictBySeq(dictSeq);
            if (dict == null) {
                return new ErrorReply("Dict not found, dict seq: " + dictSeq);
            }

            var userHome = System.getProperty("user.home");
            var file = new File(new File(userHome), "redis-d200-dict.txt");
            try {
                FileUtils.writeByteArrayToFile(file, dict.getDictBytes());
                log.info("Output dict bytes to file: {}", file.getAbsolutePath());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }

            var keyBytes = data[3];
            var valueBytes = get(keyBytes, slotPreferParsed(keyBytes));
            if (valueBytes == null) {
                return NilReply.INSTANCE;
            }

            var file2 = new File(new File(userHome), "redis-d200-value.txt");
            try {
                FileUtils.writeByteArrayToFile(file2, valueBytes);
                log.info("Output value bytes to file: {}", file2.getAbsolutePath());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }

            return OKReply.INSTANCE;
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

            ArrayList<Promise<Void>> promises = new ArrayList<>();
            var oneSlots = localPersist.oneSlots();
            for (var oneSlot : oneSlots) {
                var p = oneSlot.asyncRun(() -> {
                    oneSlot.updateDynConfig(configKey, configValueBytes);
                });
                promises.add(p);
            }

            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            Promises.all(promises).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("manage dyn-config set error: {}", e.getMessage());
                    finalPromise.setException(e);
                    return;
                }

                finalPromise.set(OKReply.INSTANCE);
            });

            return asyncReply;
        }

        return NilReply.INSTANCE;
    }
}
