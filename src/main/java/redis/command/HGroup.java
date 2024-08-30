
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.Dict;
import redis.TrainSampleJob;
import redis.reply.*;
import redis.type.RedisHH;
import redis.type.RedisHashKeys;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Random;

public class HGroup extends BaseCommand {
    public HGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("hdel".equals(cmd) || "hexists".equals(cmd) || "hget".equals(cmd) || "hgetall".equals(cmd) ||
                "hincrby".equals(cmd) || "hincrbyfloat".equals(cmd) || "hkeys".equals(cmd) || "hlen".equals(cmd) ||
                "hmget".equals(cmd) || "hmset".equals(cmd) || "hrandfield".equals(cmd) ||
                "hset".equals(cmd) || "hsetnx".equals(cmd) ||
                "hstrlen".equals(cmd) || "hvals".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
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

        return NilReply.INSTANCE;
    }

    private RedisHashKeys getRedisHashKeys(byte[] keyBytes) {
        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var keysValueBytes = get(keysKeyBytes, null, false, CompressedValue.SP_TYPE_HASH);
        if (keysValueBytes == null) {
            return null;
        }

        return RedisHashKeys.decode(keysValueBytes);
    }

    private void saveRedisHashKeys(RedisHashKeys rhk, String key) {
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();
        var slotWithKeyHashForKeys = slot(keysKeyBytes);

        if (rhk.size() == 0) {
            removeDelay(slotWithKeyHashForKeys.slot(), slotWithKeyHashForKeys.bucketIndex(), keysKey, slotWithKeyHashForKeys.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(keysKeyBytes, rhk.encode(preferDict), slotWithKeyHashForKeys, CompressedValue.SP_TYPE_HASH);
    }

    private RedisHH getRedisHH(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return null;
        }

        return RedisHH.decode(encodedBytes);
    }

    private void saveRedisHH(RedisHH rhh, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var key = new String(keyBytes);
        if (rhh.size() == 0) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(keyBytes, rhh.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_HH);
    }

    @VisibleForTesting
    boolean isUseHH(byte[] keyBytes) {
        int checkPrefixLength = RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX.length;
        if (keyBytes.length > checkPrefixLength) {
            // check prefix match
            if (Arrays.equals(keyBytes, 0, checkPrefixLength,
                    RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX, 0, checkPrefixLength)) {
                return false;
            }
        }
        return localPersist.getIsHashSaveMemberTogether();
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hdel2(keyBytes, fields);
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisHashKeys(keyBytes);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        var key = new String(keyBytes);
        int removed = 0;
        for (var field : fields) {
            if (rhk.remove(field)) {
                removed++;

                var fieldKey = RedisHashKeys.fieldKey(key, field);
                var slotWithKeyHashThisField = slot(fieldKey.getBytes());
                var bucketIndex = slotWithKeyHashThisField.bucketIndex();
                var keyHash = slotWithKeyHashThisField.keyHash();
                removeDelay(slotWithKeyHash.slot(), bucketIndex, fieldKey, keyHash);
            }
        }

        saveRedisHashKeys(rhk, key);
        return new IntegerReply(removed);
    }

    @VisibleForTesting
    Reply hdel2(byte[] keyBytes, ArrayList<String> fields) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            return IntegerReply.REPLY_0;
        }

        var removed = 0;
        for (var field : fields) {
            if (rhh.remove(field)) {
                removed++;
            }
        }

        saveRedisHH(rhh, keyBytes, slotWithKeyHash);
        return new IntegerReply(removed);
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hexists2(keyBytes, fieldBytes);
        }

        var key = new String(keyBytes);
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var fieldCv = getCv(fieldKey.getBytes());
        return fieldCv != null ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;

//        var rhk = getRedisHashKeys(keyBytes);
//        if (rhk == null) {
//            return IntegerReply.REPLY_0;
//        }
//
//        return rhk.contains(new String(fieldBytes)) ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    @VisibleForTesting
    Reply hexists2(byte[] keyBytes, byte[] fieldBytes) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return IntegerReply.REPLY_0;
        }

        final String[] fieldToFind = {new String(fieldBytes)};
        final boolean[] isFind = {false};
        RedisHH.iterate(encodedBytes, true, (field, value) -> {
            if (field.equals(fieldToFind[0])) {
                isFind[0] = true;
                return true;
            }
            return false;
        });

        return isFind[0] ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hget2(keyBytes, fieldBytes, onlyReturnLength);
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

    @VisibleForTesting
    Reply hget2(byte[] keyBytes, byte[] fieldBytes, boolean onlyReturnLength) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }

        final String[][] fieldToFind = {{new String(fieldBytes)}};
        final byte[][] fieldValueBytes = {null};
        RedisHH.iterate(encodedBytes, true, (field, value) -> {
            if (field.equals(fieldToFind[0][0])) {
                fieldValueBytes[0] = value;
                return true;
            }
            return false;
        });

        var fieldValueByte = fieldValueBytes[0];
        if (fieldValueByte == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }
        return onlyReturnLength ? new IntegerReply(fieldValueByte.length) : new BulkReply(fieldValueByte);
    }

    @VisibleForTesting
    Reply hgetall() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hgetall2(keyBytes);
        }

        var rhk = getRedisHashKeys(keyBytes);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var set = rhk.getSet();
        if (set.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var key = new String(keyBytes);
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

    @VisibleForTesting
    Reply hgetall2(byte[] keyBytes) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            return MultiBulkReply.EMPTY;
        }

        var map = rhh.getMap();
        if (map.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[map.size() * 2];
        int i = 0;
        for (var entry : map.entrySet()) {
            replies[i++] = new BulkReply(entry.getKey().getBytes());
            replies[i++] = new BulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hincrby2(keyBytes, fieldBytes, by, byFloat, isFloat);
        }

        var key = new String(keyBytes);
        var field = new String(fieldBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);

        byte[][] dd = {null, fieldKey.getBytes()};
        var dGroup = new DGroup(cmd, dd, socket);
        dGroup.from(this);
        dGroup.setSlotWithKeyHashListParsed(DGroup.parseSlots("decrby", dd, slotNumber));

        if (isFloat) {
            return dGroup.decrBy(0, -byFloat);
        } else {
            return dGroup.decrBy(-by, 0);
        }
    }

    @VisibleForTesting
    Reply hincrby2(byte[] keyBytes, byte[] fieldBytes, int by, double byFloat, boolean isByFloat) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        var field = new String(fieldBytes);
        var fieldValueBytes = rhh.get(field);

        if (fieldValueBytes == null) {
            if (isByFloat) {
                var newValue = BigDecimal.valueOf(byFloat).setScale(2, RoundingMode.UP);
                var newValueBytes = newValue.toPlainString().getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, keyBytes, slotWithKeyHash);
                return new BulkReply(newValueBytes);
            } else {
                var newValueBytes = String.valueOf(by).getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, keyBytes, slotWithKeyHash);
                return new IntegerReply(by);
            }
        } else {
            final var NOT_NUMBER_REPLY = isByFloat ? ErrorReply.NOT_FLOAT : ErrorReply.NOT_INTEGER;

            double doubleValue = 0;
            long longValue = 0;
            try {
                var numberStr = new String(fieldValueBytes);
                if (isByFloat) {
                    doubleValue = Double.parseDouble(numberStr);
                } else {
                    longValue = Long.parseLong(numberStr);
                }
            } catch (NumberFormatException e) {
                return NOT_NUMBER_REPLY;
            }

            if (isByFloat) {
                var newValue = BigDecimal.valueOf(doubleValue).setScale(2, RoundingMode.UP)
                        .add(BigDecimal.valueOf(byFloat).setScale(2, RoundingMode.UP));
                var newValueBytes = newValue.toPlainString().getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, keyBytes, slotWithKeyHash);
                return new BulkReply(newValueBytes);
            } else {
                long newValue = longValue + by;
                var newValueBytes = String.valueOf(newValue).getBytes();
                rhh.put(field, newValueBytes);

                saveRedisHH(rhh, keyBytes, slotWithKeyHash);
                return new IntegerReply(newValue);
            }
        }
    }

    @VisibleForTesting
    Reply hkeys(boolean onlyReturnSize) {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hkeys2(keyBytes, onlyReturnSize);
        }

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var keysValueBytes = get(keysKeyBytes);
        if (keysValueBytes == null) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        if (onlyReturnSize) {
            var size = RedisHashKeys.getSizeWithoutDecode(keysValueBytes);
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

    @VisibleForTesting
    Reply hkeys2(byte[] keyBytes, boolean onlyReturnSize) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_HH);
        if (encodedBytes == null) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        var size = RedisHH.getSizeWithoutDecode(encodedBytes);
        if (size == 0) {
            return onlyReturnSize ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        if (onlyReturnSize) {
            return new IntegerReply(size);
        }

        var replies = new Reply[size];
        final int[] i = {0};
        RedisHH.iterate(encodedBytes, true, (field, value) -> {
            replies[i[0]++] = new BulkReply(field.getBytes());
            return false;
        });
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hmget2(keyBytes, fields);
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

    @VisibleForTesting
    Reply hmget2(byte[] keyBytes, ArrayList<String> fields) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);

        var replies = new Reply[fields.size()];
        int i = 0;
        for (var field : fields) {
            var fieldValueBytes = rhh == null ? null : rhh.get(field);
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
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

            fieldValues.put(new String(fieldBytes), fieldValueBytes);
        }

        if (isUseHH(keyBytes)) {
            return hmset2(keyBytes, fieldValues);
        }

        var rhk = getRedisHashKeys(keyBytes);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        var key = new String(keyBytes);
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

        saveRedisHashKeys(rhk, key);
        return OKReply.INSTANCE;
    }

    @VisibleForTesting
    Reply hmset2(byte[] keyBytes, LinkedHashMap<String, byte[]> fieldValues) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        for (var entry : fieldValues.entrySet()) {
            if (rhh.size() >= RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.HASH_SIZE_TO_LONG;
            }

            rhh.put(entry.getKey(), entry.getValue());
        }

        saveRedisHH(rhh, keyBytes, slotWithKeyHash);
        return OKReply.INSTANCE;
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hrandfield2(keyBytes, count, withValues);
        }

        var rhk = getRedisHashKeys(keyBytes);
        if (rhk == null) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var set = rhk.getSet();
        if (set.isEmpty()) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = set.size();
        if (count > size) {
            count = size;
        }
        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = getRandIndex(count, size, absCount);

        var key = new String(keyBytes);
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

    @VisibleForTesting
    Reply hrandfield2(byte[] keyBytes, int count, boolean withValues) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var map = rhh.getMap();
        if (map.isEmpty()) {
            return withValues ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = map.size();
        if (count > size) {
            count = size;
        }
        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = getRandIndex(count, size, absCount);

        var replies = new Reply[withValues ? absCount * 2 : absCount];
        int i = 0;
        int j = 0;
        for (var entry : map.entrySet()) {
            if (indexes.contains(j)) {
                var field = entry.getKey();
                var fieldValueBytes = entry.getValue();
                replies[i++] = new BulkReply(field.getBytes());
                if (withValues) {
                    replies[i++] = new BulkReply(fieldValueBytes);
                }
            }
            j++;
        }
        return new MultiBulkReply(replies);
    }

    @NotNull
    static ArrayList<Integer> getRandIndex(int count, int size, int absCount) {
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
        return indexes;
    }

    @VisibleForTesting
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

        if (isUseHH(keyBytes)) {
            return hsetnx2(keyBytes, fieldBytes, fieldValueBytes);
        }

        var rhk = getRedisHashKeys(keyBytes);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        var field = new String(fieldBytes);
        if (rhk.contains(field)) {
            return IntegerReply.REPLY_0;
        }

        var key = new String(keyBytes);
        var fieldKey = RedisHashKeys.fieldKey(key, field);
        var fieldKeyBytes = fieldKey.getBytes();

        var slotWithKeyHashThisField = slot(fieldKeyBytes);

        var fieldCv = getCv(fieldKeyBytes, slotWithKeyHashThisField);
        if (fieldCv != null) {
            throw new IllegalStateException("Hash field cv exists, key: " + key + ", field: " + field);
//            return IntegerReply.REPLY_0;
        }

        set(fieldKeyBytes, fieldValueBytes, slotWithKeyHashThisField);
        rhk.add(field);
        saveRedisHashKeys(rhk, key);
        return IntegerReply.REPLY_1;
    }

    @VisibleForTesting
    Reply hsetnx2(byte[] keyBytes, byte[] fieldBytes, byte[] fieldValueBytes) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            rhh = new RedisHH();
        }

        var field = new String(fieldBytes);
        if (rhh.get(field) != null) {
            return IntegerReply.REPLY_0;
        }

        rhh.put(field, fieldValueBytes);
        saveRedisHH(rhh, keyBytes, slotWithKeyHash);
        return IntegerReply.REPLY_1;
    }

    @VisibleForTesting
    Reply hvals() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (isUseHH(keyBytes)) {
            return hvals2(keyBytes);
        }

        var rhk = getRedisHashKeys(keyBytes);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var set = rhk.getSet();
        if (set.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var key = new String(keyBytes);
        var replies = new Reply[set.size()];
        int i = 0;
        for (var field : set) {
            var fieldKey = RedisHashKeys.fieldKey(key, field);
            var fieldValueBytes = get(fieldKey.getBytes());
            replies[i++] = fieldValueBytes == null ? NilReply.INSTANCE : new BulkReply(fieldValueBytes);
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply hvals2(byte[] keyBytes) {
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhh = getRedisHH(keyBytes, slotWithKeyHash);
        if (rhh == null) {
            return MultiBulkReply.EMPTY;
        }

        var map = rhh.getMap();
        if (map.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[map.size()];
        int i = 0;
        for (var entry : map.entrySet()) {
            replies[i++] = new BulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }
}
