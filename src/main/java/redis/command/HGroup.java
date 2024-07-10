
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.TrainSampleJob;
import redis.reply.*;
import redis.type.RedisHashKeys;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;
import static redis.TrainSampleJob.MIN_TRAIN_SAMPLE_SIZE;

public class HGroup extends BaseCommand {
    public HGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("hdel".equals(cmd) || "hexists".equals(cmd) || "hget".equals(cmd) || "hgetall".equals(cmd) ||
                "hincrby".equals(cmd) || "hincrbyfloat".equals(cmd) || "hkeys".equals(cmd) || "hlen".equals(cmd) ||
                "hmget".equals(cmd) || "hmset".equals(cmd) || "hrandfield".equals(cmd) ||
                "hset".equals(cmd) || "hsetnx".equals(cmd) ||
                "hstrlen".equals(cmd) || "hvals".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("hdel".equals(cmd)) {
            return hdel();
        }

        if ("hexists".equals(cmd)) {
            return hexists();
        }

        if ("hget".equals(cmd)) {
            return hget(false);
        }

        if ("hgetall".equals(cmd)) {
            return hgetall();
        }

        if ("hincrby".equals(cmd)) {
            return hincrby(false);
        }

        if ("hincrbyfloat".equals(cmd)) {
            return hincrby(true);
        }

        if ("hkeys".equals(cmd)) {
            return hkeys(false);
        }

        if ("hlen".equals(cmd)) {
            return hkeys(true);
        }

        if ("hmget".equals(cmd)) {
            return hmget();
        }

        if ("hmset".equals(cmd) || "hset".equals(cmd)) {
            return hmset();
        }

        if ("hrandfield".equals(cmd)) {
            return hrandfield();
        }

        if ("hsetnx".equals(cmd)) {
            return hsetnx();
        }

        if ("hstrlen".equals(cmd)) {
            return hget(true);
        }

        if ("hvals".equals(cmd)) {
            return hvals();
        }

        if ("h_field_dict_train".equals(cmd)) {
            return h_field_dict_train();
        }

        return NilReply.INSTANCE;
    }

    Reply hdel() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        ArrayList<String> fields = new ArrayList<>();
        for (int i = 2; i < data.length; i++) {
            var fieldBytes = data[i];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            fields.add(new String(fieldBytes));
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();
        var slotWithKeyHashForKeys = slot(keysKeyBytes);
        var slot = slotWithKeyHashForKeys.slot();

        var keysValueBytes = get(keysKeyBytes, slotWithKeyHashForKeys);
        if (keysValueBytes == null) {
            return IntegerReply.REPLY_0;
        }

        int removed = 0;
        var rhk = RedisHashKeys.decode(keysValueBytes);
        for (var field : fields) {
            if (rhk.remove(field)) {
                removed++;

                var fieldKey = RedisHashKeys.fieldKey(key, field);
                var slotWithKeyHashThisField = slot(fieldKey.getBytes());
                var bucketIndex = slotWithKeyHashThisField.bucketIndex();
                var keyHash = slotWithKeyHashThisField.keyHash();
                removeDelay(slot, bucketIndex, fieldKey, keyHash);
            }
        }

        var encodedBytes = rhk.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_HASH_COMPRESSED : CompressedValue.SP_TYPE_HASH;

        set(keysKeyBytes, encodedBytes, slotWithKeyHashForKeys, spType);
        return new IntegerReply(removed);
    }

    Reply hexists() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var key = new String(keyBytes);
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var fieldCv = getCv(fieldKey.getBytes());
        return fieldCv != null ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;

//        var keysKey = RedisHashKeys.keysKey(key);
//        var keysKeyBytes = keysKey.getBytes();
//        var slotWithKeyHashForKeys = slot(keysKeyBytes);
//
//        var encodedBytes = get(keysKeyBytes, slotWithKeyHashForKeys);
//        if (encodedBytes == null) {
//            return IntegerReply.REPLY_0;
//        }
//
//        var rhk = RedisHashKeys.decode(encodedBytes);
//        return rhk.contains(new String(fieldBytes)) ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    Reply hget(boolean onlyReturnLength) {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var key = new String(keyBytes);
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var fieldCv = getCv(fieldKey.getBytes());

        if (fieldCv == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }

        var fieldValueBytes = getValueBytesByCv(fieldCv);
        return onlyReturnLength ? new IntegerReply(fieldValueBytes.length) : new BulkReply(fieldValueBytes);
    }

    Reply hgetall() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var keysValueBytes = get(keysKeyBytes);
        if (keysValueBytes == null) {
            return MultiBulkReply.EMPTY;
        }

        var rhk = RedisHashKeys.decode(keysValueBytes);
        var set = rhk.getSet();
        if (set.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[set.size() * 2];
        int i = 0;
        for (var field : set) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldCv = getCv(fieldKey.getBytes());
            replies[i++] = new BulkReply(field.getBytes());
            replies[i++] = fieldCv == null ? NilReply.INSTANCE : new BulkReply(getValueBytesByCv(fieldCv));
        }
        return new MultiBulkReply(replies);
    }

    Reply hincrby(boolean isFloat) {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];
        var byBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int by = 0;
        double byFloat = 0;
        if (isFloat) {
            try {
                byFloat = Double.parseDouble(new String(byBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }
        } else {
            try {
                by = Integer.parseInt(new String(byBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var key = new String(keyBytes);
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);

        byte[][] dd = {null, fieldKey.getBytes()};
        var dGroup = new DGroup(cmd, dd, socket);
        dGroup.from(this);

        if (isFloat) {
            return dGroup.decrBy(0, -byFloat);
        } else {
            return dGroup.decrBy(-by, 0);
        }
    }

    Reply hkeys(boolean onlyReturnSize) {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var keysValueBytes = get(keysKeyBytes);
        if (keysValueBytes == null) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        // need not decode, just return size
        if (onlyReturnSize) {
            var size = ByteBuffer.wrap(keysValueBytes).getShort();
            return new IntegerReply(size);
        }

        var rhk = RedisHashKeys.decode(keysValueBytes);
        var set = rhk.getSet();
        if (set.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[set.size()];
        int i = 0;
        for (var field : set) {
            replies[i++] = new BulkReply(field.getBytes());
        }
        return new MultiBulkReply(replies);
    }

    Reply hmget() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var fields = new ArrayList<String>();
        for (int i = 2; i < data.length; i++) {
            var fieldBytes = data[i];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            fields.add(new String(fieldBytes));
        }

        var key = new String(keyBytes);

        var replies = new Reply[fields.size()];
        int i = 0;
        for (var field : fields) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = get(fieldKey.getBytes());
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    Reply hmset() {
        if (data.length < 4 || data.length % 2 != 0) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

//        var dictMap = DictMap.getInstance();
        LinkedHashMap<String, byte[]> fieldValues = new LinkedHashMap<>();
        for (int i = 2; i < data.length; i += 2) {
            var fieldBytes = data[i];
            var fieldValueBytes = data[i + 1];
            if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            if (fieldValueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
                return ErrorReply.VALUE_TOO_LONG;
            }

            // compress field value bytes
//            if (fieldValueBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH) {
//                final String keyPrefixGiven = new String(fieldBytes);
//
//                var dict = dictMap.getDict(keyPrefixGiven);
//                var beginT = System.nanoTime();
//                var fieldCv = CompressedValue.compress(fieldValueBytes, dict, compressLevel);
//                var costT = (System.nanoTime() - beginT) / 1000;
//                if (costT == 0) {
//                    costT = 1;
//                }
//
//                int compressedLength = fieldCv.getCompressedLength();
//
//                // stats
//                compressStats.compressedCount++;
//                compressStats.compressedTotalLength += compressedLength;
//                compressStats.compressedCostTimeTotalUs += costT;
//
//                int encodedCvLength = RedisHH.PREFER_COMPRESS_FIELD_MAGIC_PREFIX.length + CompressedValue.VALUE_HEADER_LENGTH + compressedLength;
//                // if compress ratio less than 0.8, use compressed bytes
//                final double preferCompressRatio = 0.8;
//                if (encodedCvLength < (fieldValueBytes.length * preferCompressRatio)) {
//                    fieldCv.setSeq(snowFlake.nextId());
//                    fieldCv.setDictSeqOrSpType(dict != null ? dict.getSeq() : CompressedValue.NULL_DICT_SEQ);
//                    fieldCv.setKeyHash(fieldCv.getSeq());
//
//                    if (dict == null) {
//                        var kv = new TrainSampleJob.TrainSampleKV(keyPrefixGiven, keyPrefixGiven, fieldCv.getSeq(), fieldValueBytes);
//                        sampleToTrainList.add(kv);
//                    }
//
//                    var fieldEncodeBytes = new byte[encodedCvLength];
//                    var buf = ByteBuf.wrapForWriting(fieldEncodeBytes);
//                    buf.put(RedisHH.PREFER_COMPRESS_FIELD_MAGIC_PREFIX);
//                    fieldCv.encodeTo(buf);
//
//                    // replace use compressed bytes
//                    fieldValueBytes = fieldEncodeBytes;
//                }
//            }

            fieldValues.put(new String(fieldBytes), fieldValueBytes);
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();
        var slotWithKeyHashForKeys = slot(keysKeyBytes);

        var keysValueBytes = get(keysKeyBytes, slotWithKeyHashForKeys);
        var rhk = keysValueBytes == null ? new RedisHashKeys() : RedisHashKeys.decode(keysValueBytes);

        for (var entry : fieldValues.entrySet()) {
            if (rhk.size() >= RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.HASH_SIZE_TO_LONG;
            }

            var field = entry.getKey();
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = entry.getValue();
            set(fieldKey.getBytes(), fieldValueBytes);

            rhk.add(field);
        }

        var encodedBytes = rhk.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_HASH_COMPRESSED : CompressedValue.SP_TYPE_HASH;

        set(keysKeyBytes, encodedBytes, slotWithKeyHashForKeys, spType);
        return OKReply.INSTANCE;
    }

    Reply hrandfield() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int count = 1;
        boolean withValues = false;
        for (int i = 2; i < data.length; i++) {
            var arg = new String(data[i]);
            if ("withvalues".equalsIgnoreCase(arg)) {
                withValues = true;
            } else {
                try {
                    count = Integer.parseInt(arg);
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
            }
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var keysValueBytes = get(keysKeyBytes);
        if (keysValueBytes == null) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var rhk = RedisHashKeys.decode(keysValueBytes);
        var set = rhk.getSet();
        if (set.isEmpty()) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = set.size();
        if (count > size) {
            count = size;
        }
        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = new ArrayList<>();
        if (count == size) {
            // need not random, return all fields
            for (int i = 0; i < size; i++) {
                indexes.add(i);
            }
        } else {
            boolean canUseSameField = count < 0;
            var rand = new Random();
            for (int i = 0; i < absCount; i++) {
                int index;
                do {
                    index = rand.nextInt(size);
                } while (!canUseSameField && indexes.contains(index));
                indexes.add(index);
            }
        }

        var replies = new Reply[withValues ? absCount * 2 : absCount];
        int i = 0;
        int j = 0;
        for (var field : set) {
            if (indexes.contains(j)) {
                replies[i++] = new BulkReply(field.getBytes());
                if (withValues) {
                    var fieldKey = RedisHashKeys.fieldKey(key, field);
                    var fieldValueBytes = get(fieldKey.getBytes());
                    replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
                }
            }
            j++;
        }
        return new MultiBulkReply(replies);
    }

    Reply hsetnx() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var fieldBytes = data[2];
        var fieldValueBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (fieldValueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        var key = new String(keyBytes);
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var fieldKeyBytes = fieldKey.getBytes();

        var slotWithKeyHashThisField = slot(fieldKeyBytes);

        var fieldCv = getCv(fieldKeyBytes, slotWithKeyHashThisField);
        if (fieldCv != null) {
            return IntegerReply.REPLY_0;
        }

        set(fieldKeyBytes, fieldValueBytes, slotWithKeyHashThisField);
        return IntegerReply.REPLY_1;
    }

    Reply hvals() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var keysValueBytes = get(keysKeyBytes);
        if (keysValueBytes == null) {
            return MultiBulkReply.EMPTY;
        }

        var rhk = RedisHashKeys.decode(keysValueBytes);
        var set = rhk.getSet();
        if (set.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[set.size()];
        int i = 0;
        for (var field : set) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = get(fieldKey.getBytes());
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    Reply h_field_dict_train() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        if (data.length <= 2 + MIN_TRAIN_SAMPLE_SIZE) {
            return new ErrorReply("Train sample size too small");
        }

        var keyPrefixGiven = new String(data[1]);

        List<TrainSampleJob.TrainSampleKV> sampleToTrainList = new ArrayList<>();
        for (int i = 2; i < data.length; i++) {
            sampleToTrainList.add(new TrainSampleJob.TrainSampleKV(null, keyPrefixGiven, 0L, data[i]));
        }

        var trainSampleJob = new TrainSampleJob(workerId);
        trainSampleJob.resetSampleToTrainList(sampleToTrainList);
        var trainSampleResult = trainSampleJob.train();

//        if (trainSampleResult == null) {
//            return new ErrorReply("Train sample fail");
//        }

        var trainSampleCacheDict = trainSampleResult.cacheDict();
        for (var entry : trainSampleCacheDict.entrySet()) {
            var dict = entry.getValue();
            dictMap.putDict(entry.getKey(), dict);
        }

        return new IntegerReply(trainSampleCacheDict.size());
    }
}
