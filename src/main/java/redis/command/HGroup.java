
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.reply.*;
import redis.type.RedisHashKeys;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Random;

import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public class HGroup extends BaseCommand {
    public HGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
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

        return NilReply.INSTANCE;
    }

    private Reply hdel() {
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

        var oneSlot = localPersist.oneSlot(slot);

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
                oneSlot.removeDelay(workerId, fieldKey, slotWithKeyHashThisField.bucketIndex(), slotWithKeyHashThisField.keyHash());
            }
        }

        var encodedBytes = rhk.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_HASH_COMPRESSED : CompressedValue.SP_TYPE_HASH;

        set(keysKeyBytes, encodedBytes, slotWithKeyHashForKeys, spType);
        return new IntegerReply(removed);
    }

    private Reply hexists() {
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
        var fieldKey = RedisHashKeys.fieldKey(key, new String(fieldBytes));
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

    private Reply hget(boolean onlyReturnLength) {
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
        var fieldKey = RedisHashKeys.fieldKey(key, new String(fieldBytes));
        var fieldCv = getCv(fieldKey.getBytes());

        if (fieldCv == null) {
            return onlyReturnLength ? IntegerReply.REPLY_0 : NilReply.INSTANCE;
        }

        var fieldValueBytes = getValueBytesByCv(fieldCv);
        return onlyReturnLength ? new IntegerReply(fieldValueBytes.length) : new BulkReply(fieldValueBytes);
    }

    private Reply hgetall() {
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

    private Reply hincrby(boolean isFloat) {
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
        var fieldKey = RedisHashKeys.fieldKey(key, new String(fieldBytes));

        byte[][] dd = {null, fieldKey.getBytes()};
        var dGroup = new DGroup(cmd, dd, socket);
        dGroup.from(this);

        if (isFloat) {
            return dGroup.decrBy(0, -byFloat);
        } else {
            return dGroup.decrBy(-by, 0);
        }
    }

    private Reply hkeys(boolean onlyReturnSize) {
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

    private Reply hmget() {
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

    private Reply hvals() {
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

    private Reply hmset() {
        if (data.length < 4 || data.length % 2 != 0) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

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

        var key = new String(keyBytes);
        var keysKey = RedisHashKeys.keysKey(key);
        var keysKeyBytes = keysKey.getBytes();

        var slotWithKeyHashForKeys = slot(keysKeyBytes);

        var keysValueBytes = get(keysKeyBytes, slotWithKeyHashForKeys);
        var rhk = keysValueBytes == null ? new RedisHashKeys() : RedisHashKeys.decode(keysValueBytes);

        int size = rhk.size();
        if (size >= RedisHashKeys.HASH_MAX_SIZE) {
            return ErrorReply.HASH_SIZE_TO_LONG;
        }

        // for debug
//      int overwrite = 0;
        for (var entry : fieldValues.entrySet()) {
            var field = entry.getKey();
            var fieldValueBytes = entry.getValue();

            var fieldKey = RedisHashKeys.fieldKey(key, field);
            set(fieldKey.getBytes(), fieldValueBytes);

            boolean isNewAdded = rhk.add(field);
            if (isNewAdded && (size + 1) == RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.HASH_SIZE_TO_LONG;
            }

//            if (!isNewAdded) {
//                overwrite++;
//            }
        }

        var encodedBytes = rhk.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_HASH_COMPRESSED : CompressedValue.SP_TYPE_HASH;

        set(keysKeyBytes, encodedBytes, slotWithKeyHashForKeys, spType);
        return OKReply.INSTANCE;
    }

    private Reply hrandfield() {
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

        var replies = new Reply[withValues ? count * 2 : count];
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

    private Reply hsetnx() {
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
        var fieldKey = RedisHashKeys.fieldKey(key, new String(fieldBytes));
        var fieldKeyBytes = fieldKey.getBytes();

        var slotWithKeyHashThisField = slot(fieldKeyBytes);

        var fieldCv = getCv(fieldKeyBytes, slotWithKeyHashThisField);
        if (fieldCv != null) {
            return IntegerReply.REPLY_0;
        }

        set(fieldKeyBytes, fieldValueBytes, slotWithKeyHashThisField);
        return IntegerReply.REPLY_1;
    }
}
