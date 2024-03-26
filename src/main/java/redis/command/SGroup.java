
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.clients.jedis.Jedis;
import redis.reply.*;
import redis.type.RedisHashKeys;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public class SGroup extends BaseCommand {
    public SGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("set".equals(cmd) || "setex".equals(cmd) || "setrange".equals(cmd) ||
                "setnx".equals(cmd) || "strlen".equals(cmd) || "substr".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        // set group, todo: check
        if ("sadd".equals(cmd) || "scard".equals(cmd) ||
                "sdiff".equals(cmd) || "sdiffstore".equals(cmd) || "sinterstore".equals(cmd) ||
                "sismember".equals(cmd) || "smembers".equals(cmd) || "smismember".equals(cmd) ||
                "spop".equals(cmd) || "srandmember".equals(cmd) || "srem".equals(cmd) ||
                "sunion".equals(cmd) || "sunionstore".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        if ("sintercard".equals(cmd)) {
            if (data.length < 3) {
                return null;
            }
            var keyBytes = data[2];
            return slot(keyBytes, slotNumber);
        }

        if ("smove".equals(cmd)) {
            if (data.length != 4) {
                return null;
            }
            var dstKeyBytes = data[2];
            return slot(dstKeyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("set".equals(cmd)) {
            return set(data);
        }

        if ("setex".equals(cmd)) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[3], "ex".getBytes(), data[2]};
            return set(dd);
        }

        if ("setnx".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[2], "nx".getBytes()};
            return set(dd);
        }

        if ("setrange".equals(cmd)) {
            return setrange();
        }

        if ("strlen".equals(cmd)) {
            return strlen();
        }

        if ("substr".equals(cmd)) {
            var gGroup = new GGroup(cmd, data, socket);
            gGroup.from(this);
            return gGroup.getrange();
        }

        if ("select".equals(cmd)) {
            return select();
        }

        if ("save".equals(cmd)) {
            // already saved when handle request
            return OKReply.INSTANCE;
        }

        // set group
        if ("sadd".equals(cmd)) {
            return sadd();
        }

        if ("scard".equals(cmd)) {
            return scard();
        }

        if ("sdiff".equals(cmd)) {
            return sdiff(false, false);
        }

        if ("sdiffstore".equals(cmd)) {
            return sdiffstore(false, false);
        }

        if ("sinter".equals(cmd)) {
            return sdiff(true, false);
        }

        if ("sintercard".equals(cmd)) {
            return sintercard();
        }

        if ("sinterstore".equals(cmd)) {
            return sdiffstore(true, false);
        }

        if ("sismember".equals(cmd)) {
            return sismember();
        }

        if ("smembers".equals(cmd)) {
            return smembers();
        }

        if ("smismember".equals(cmd)) {
            return smismember();
        }

        if ("smove".equals(cmd)) {
            return smove();
        }

        if ("spop".equals(cmd)) {
            return srandmember(true);
        }

        if ("srandmember".equals(cmd)) {
            return srandmember(false);
        }

        if ("srem".equals(cmd)) {
            return srem();
        }

        if ("sunion".equals(cmd)) {
            return sdiff(false, true);
        }

        if ("sunionstore".equals(cmd)) {
            return sdiffstore(false, true);
        }

        if ("slaveof".equals(cmd)) {
            return slaveof();
        }

        return NilReply.INSTANCE;
    }

    private static final String IPV4_REGEX =
            "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";

    private static final Pattern IPv4_PATTERN = Pattern.compile(IPV4_REGEX);

    // execute in a net thread
    private Reply slaveof() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var hostBytes = data[1];
        var portBytes = data[2];

        var isNoOne = "no".equalsIgnoreCase(new String(hostBytes));
        if (isNoOne) {
            CompletableFuture<Boolean>[] futures = new CompletableFuture[slotNumber];
            for (int i = 0; i < slotNumber; i++) {
                var oneSlot = localPersist.oneSlot((byte) i);
                futures[i] = oneSlot.threadSafeHandle(() -> {
                    try {
                        oneSlot.removeReplPairAsSlave();
                        return true;
                    } catch (Exception e) {
                        log.error("Remove repl pair as slave failed, slot: " + oneSlot.slot(), e);
                        return false;
                    }
                });
            }
            CompletableFuture.allOf(futures).join();

            return OKReply.INSTANCE;
        }

        var host = new String(hostBytes);
        // host valid check
        var matcher = IPv4_PATTERN.matcher(host);
        if (!matcher.matches()) {
            return ErrorReply.SYNTAX;
        }

        int port;
        try {
            port = Integer.parseInt(new String(portBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        // port range check
        if (port < 0 || port > 65535) {
            return new ErrorReply("invalid port");
        }

        // connect check
        Jedis jedis = null;
        try {
            jedis = new Jedis(host, port);
            var pong = jedis.ping();
            log.info("Slave of {}:{} pong: {}", host, port, pong);
        } catch (Exception e) {
            return new ErrorReply("connect failed");
        } finally {
            jedis.close();
        }

        CompletableFuture<Boolean>[] futures = new CompletableFuture[slotNumber];
        for (int i = 0; i < slotNumber; i++) {
            var oneSlot = localPersist.oneSlot((byte) i);
            futures[i] = oneSlot.threadSafeHandle(() -> {
                try {
                    oneSlot.createReplPairAsSlave(host, port);
                    return true;
                } catch (Exception e) {
                    log.error("Create repl pair as slave failed, slot: " + oneSlot.slot(), e);
                    return false;
                }
            });
        }
        CompletableFuture.allOf(futures).join();

        return OKReply.INSTANCE;
    }

    Reply set(byte[][] dd) {
        // add rate limit by request workers/merge workers, todo
        if (dd.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = dd[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        // for local test, random value, test compress ratio
        var valueBytes = dd[2];
        if (localTest) {
            int randomValueIndex = new Random().nextInt(localTestRandomValueList.size());
            var randomValueBytes = localTestRandomValueList.get(randomValueIndex);

            valueBytes = new byte[randomValueBytes.length];

            // change last 16 bytes same as key
            System.arraycopy(randomValueBytes, 0, valueBytes, 0, valueBytes.length - keyBytes.length);
            System.arraycopy(keyBytes, 0, valueBytes, valueBytes.length - keyBytes.length, keyBytes.length);
        }

        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        boolean isNx = false;
        boolean isXx = false;
        long ex = -1;
        long px = -1;
        long exAt = -1;
        long pxAt = -1;
        boolean isExpireAtSet = false;
        boolean isKeepTtl = false;
        boolean isReturnExist = false;
        for (int i = 3; i < dd.length; i++) {
            var arg = new String(dd[i]);
            isNx = "nx".equalsIgnoreCase(arg);
            isXx = "xx".equalsIgnoreCase(arg);
            if (isNx || isXx) {
                continue;
            }

            isKeepTtl = "keepttl".equalsIgnoreCase(arg);
            if (isKeepTtl) {
                continue;
            }

            isReturnExist = "get".equalsIgnoreCase(arg);
            if (isReturnExist) {
                continue;
            }

            boolean isEx = "ex".equalsIgnoreCase(arg);
            boolean isPx = "px".equalsIgnoreCase(arg);
            boolean isExAt = "exat".equalsIgnoreCase(arg);
            boolean isPxAt = "pxat".equalsIgnoreCase(arg);

            isExpireAtSet = isEx || isPx || isExAt || isPxAt;
            if (!isExpireAtSet) {
                continue;
            }

            if (dd.length <= i + 1) {
                return ErrorReply.SYNTAX;
            }
            long value;
            try {
                value = Long.parseLong(new String(dd[i + 1]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (isEx) {
                ex = value;
            } else if (isPx) {
                px = value;
            } else if (isExAt) {
                exAt = value;
            } else if (isPxAt) {
                pxAt = value;
            }

            i++;
        }

        long expireAt = NO_EXPIRE;
        if (isExpireAtSet) {
            if (ex != -1) {
                expireAt = System.currentTimeMillis() + ex * 1000;
            } else if (px != -1) {
                expireAt = System.currentTimeMillis() + px;
            } else if (exAt != -1) {
                expireAt = exAt * 1000;
            } else if (pxAt != -1) {
                expireAt = pxAt;
            }
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);

        CompressedValue cv = null;
        if (isReturnExist || isNx || isXx || isKeepTtl) {
            cv = getCv(keyBytes, slotWithKeyHash);
            boolean isOldExist = cv != null;
            if (isNx && isOldExist) {
                return NilReply.INSTANCE;
            }
            if (isXx && !isOldExist) {
                return NilReply.INSTANCE;
            }

            // check if not string type
            if (isOldExist && isReturnExist && !cv.isString()) {
                log.debug("Key {} is not string type", new String(keyBytes));
                return NilReply.INSTANCE;
            }

            // keep ttl
            if (isOldExist && isKeepTtl) {
                expireAt = cv.getExpireAt();
            }

            set(keyBytes, valueBytes, slotWithKeyHash, 0, expireAt);
        } else {
            set(keyBytes, valueBytes, slotWithKeyHash, 0, expireAt);
        }

        if (isReturnExist) {
            if (cv == null) {
                return NilReply.INSTANCE;
            } else {
                if (!cv.isString()) {
                    return ErrorReply.NOT_STRING;
                }
                return new BulkReply(getValueBytesByCv(cv));
            }
        }

        return OKReply.INSTANCE;
    }

    private Reply setrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var offsetBytes = data[2];
        var valueBytes = data[3];

        int offset;
        try {
            offset = Integer.parseInt(new String(offsetBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (offset < 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);

        int lengthResult = 0;
        var valueBytesExist = get(keyBytes, slotWithKeyHash);
        int len = offset + valueBytes.length;
        if (valueBytesExist == null) {
            lengthResult = len;

            // padding 0
            var setBytes = new byte[len];
            System.arraycopy(valueBytes, 0, setBytes, offset, valueBytes.length);

            set(keyBytes, setBytes, slotWithKeyHash);
        } else {
            int maxLength = Math.max(valueBytesExist.length, len);
            lengthResult = maxLength;

            var setBytes = new byte[maxLength];
            System.arraycopy(valueBytes, 0, setBytes, offset, valueBytes.length);
            if (maxLength > len) {
                System.arraycopy(valueBytesExist, len, setBytes, len, maxLength - len);
            }
            if (offset > 0) {
                int minLength = Math.min(valueBytesExist.length, offset);
                System.arraycopy(valueBytesExist, 0, setBytes, 0, minLength);
            }

            set(keyBytes, setBytes, slotWithKeyHash);
        }
        return new IntegerReply(lengthResult);
    }

    private Reply strlen() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var cv = getCv(keyBytes, slotPreferParsed(keyBytes));
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }
        return new IntegerReply(cv.uncompressedLength());
    }

    private Reply select() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        try {
//            int index = Integer.parseInt(new String(data[1]));
            // todo
            return OKReply.INSTANCE;
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
    }

    private Reply sadd() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        byte[][] memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        // use RedisHashKeys to store set
        var slotWithKeyHash = slotPreferParsed(keyBytes);

        var setCv = getCv(keyBytes, slotWithKeyHash);
        var setValueBytes = setCv != null && setCv.isSet() ? getValueBytesByCv(setCv) : null;
        var rhk = setValueBytes == null ? new RedisHashKeys() : RedisHashKeys.decode(setValueBytes);
        var size = rhk.size();

        if (size >= RedisHashKeys.HASH_MAX_SIZE) {
            return ErrorReply.SET_SIZE_TO_LONG;
        }

        int added = 0;
        for (var memberBytes : memberBytesArr) {
            boolean isNewAdded = rhk.add(new String(memberBytes));
            if (isNewAdded && (size + 1) == RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.SET_SIZE_TO_LONG;
            }
            if (isNewAdded) {
                added++;
            }
        }

        var encodedBytes = rhk.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

        set(keyBytes, encodedBytes, slotWithKeyHash, spType);
        return new IntegerReply(added);
    }

    private Reply scard() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var setCv = getCv(keyBytes, slotPreferParsed(keyBytes));
        if (setCv == null) {
            return IntegerReply.REPLY_0;
        }

        if (!setCv.isSet()) {
            return ErrorReply.WRONG_TYPE;
        }

        var setValueBytes = getValueBytesByCv(setCv);
        if (setValueBytes == null) {
            return IntegerReply.REPLY_0;
        }

        int size = ByteBuffer.wrap(setValueBytes).getShort();
        return new IntegerReply(size);
    }

    private RedisHashKeys getByKeyBytes(byte[] keyBytes) {
        return getByKeyBytes(keyBytes, null);
    }

    private RedisHashKeys getByKeyBytes(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        var setCv = getCv(keyBytes, slotWithKeyHashReuse);
        if (setCv == null) {
            return null;
        }
        if (!setCv.isSet()) {
            // throw exception ?
            return null;
        }

        var setValueBytes = getValueBytesByCv(setCv);
        if (setValueBytes == null) {
            return null;
        }
        return RedisHashKeys.decode(setValueBytes);
    }

    private Reply sdiff(boolean isInter, boolean isUnion) {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        byte[][] otherKeyBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var otherKeyBytes = data[i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i - 2] = otherKeyBytes;
        }

        var rhk = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var set = rhk.getSet();
        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRhk = getByKeyBytes(otherKeyBytes);
            if (otherRhk != null) {
                if (isInter) {
                    set.retainAll(otherRhk.getSet());
                } else if (isUnion) {
                    set.addAll(otherRhk.getSet());
                } else {
                    set.removeAll(otherRhk.getSet());
                }
                if (set.isEmpty()) {
                    break;
                }
            } else {
                if (isInter) {
                    set.clear();
                    break;
                }
            }
        }

        if (set.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[set.size()];
        int i = 0;
        for (var value : set) {
            replies[i++] = new BulkReply(value.getBytes());
        }
        return new MultiBulkReply(replies);
    }

    private Reply sdiffstore(boolean isInter, boolean isUnion) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var dstKeyBytes = data[1];
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var keyBytes = data[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        byte[][] otherKeyBytesArr = new byte[data.length - 3][];
        for (int i = 3; i < data.length; i++) {
            var otherKeyBytes = data[i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i - 3] = otherKeyBytes;
        }

        // in bucket lock?
        var rhk = getByKeyBytes(keyBytes);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        var set = rhk.getSet();
        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRhk = getByKeyBytes(otherKeyBytes);
            if (otherRhk != null) {
                if (isInter) {
                    set.retainAll(otherRhk.getSet());
                } else if (isUnion) {
                    set.addAll(otherRhk.getSet());
                } else {
                    set.removeAll(otherRhk.getSet());
                }
                if (set.isEmpty()) {
                    break;
                }
            } else {
                if (isInter) {
                    set.clear();
                    break;
                }
            }
        }

        var slotWithKeyHashForDst = slotPreferParsed(dstKeyBytes);
        var slot = slotWithKeyHashForDst.slot();

        var oneSlot = localPersist.oneSlot(slot);
        if (set.isEmpty()) {
            // remove dst key
            oneSlot.removeDelay(workerId, new String(dstKeyBytes), slotWithKeyHashForDst.bucketIndex(), slotWithKeyHashForDst.keyHash());
        } else {
            var encodedBytes = rhk.encode();
            var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
            var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

            set(dstKeyBytes, encodedBytes, slotWithKeyHashForDst, spType);
        }

        return new IntegerReply(set.size());
    }

    private Reply sintercard() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var numkeysBytes = data[1];
        int numkeys;
        try {
            numkeys = Integer.parseInt(new String(numkeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numkeys < 2) {
            return ErrorReply.INVALID_INTEGER;
        }

        var keyBytes = data[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var otherKeyBytesArr = new byte[numkeys - 1][];
        for (int i = 3; i < otherKeyBytesArr.length + 3; i++) {
            var otherKeyBytes = data[i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i - 3] = otherKeyBytes;
        }

        int limit = 0;
        // limit
        if (data.length > numkeys + 2) {
            if (data.length != numkeys + 4) {
                return ErrorReply.SYNTAX;
            }

            var limitFlagBytes = data[numkeys + 2];
            if (!"limit".equals(new String(limitFlagBytes))) {
                return ErrorReply.SYNTAX;
            }

            var limitBytes = data[numkeys + 3];
            try {
                limit = Integer.parseInt(new String(limitBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var rhk = getByKeyBytes(keyBytes);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        var set = rhk.getSet();
        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRhk = getByKeyBytes(otherKeyBytes);
            if (otherRhk != null) {
                set.retainAll(otherRhk.getSet());
                if (set.isEmpty()) {
                    break;
                }
                if (limit != 0 && set.size() >= limit) {
                    break;
                }
            } else {
                set.clear();
                break;
            }
        }

        int min = limit != 0 ? Math.min(set.size(), limit) : set.size();
        return new IntegerReply(min);
    }

    private Reply sismember() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[2];
        if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
            return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
        }

        var rhk = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        boolean isMember = rhk.contains(new String(memberBytes));
        return isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    private Reply smembers() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var rhk = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var set = rhk.getSet();
        var replies = new Reply[set.size()];
        int i = 0;
        for (var value : set) {
            replies[i++] = new BulkReply(value.getBytes());
        }
        return new MultiBulkReply(replies);
    }

    private Reply smismember() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        byte[][] memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        var rhk = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[memberBytesArr.length];
        for (int i = 0; i < memberBytesArr.length; i++) {
            boolean isMember = rhk.contains(new String(memberBytesArr[i]));
            replies[i] = isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }
        return new MultiBulkReply(replies);
    }

    private Reply smove() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        if (srcKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var dstKeyBytes = data[2];
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[3];
        if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
            return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
        }

        var slotWithKeyHashForSrc = slot(srcKeyBytes);
        var slotSrc = slotWithKeyHashForSrc.slot();
        var oneSlotSrc = localPersist.oneSlot(slotSrc);

        var slotWithKeyHashForDst = slot(dstKeyBytes);

        var srcRhk = getByKeyBytes(srcKeyBytes, slotWithKeyHashForSrc);
        if (srcRhk == null) {
            return IntegerReply.REPLY_0;
        }

        var member = new String(memberBytes);
        boolean isMember = srcRhk.remove(member);
        if (!isMember) {
            return IntegerReply.REPLY_0;
        }

        if (srcRhk.size() == 0) {
            // remove key
            oneSlotSrc.removeDelay(workerId, new String(srcKeyBytes), slotWithKeyHashForSrc.bucketIndex(), slotWithKeyHashForSrc.keyHash());
        } else {
            var encodedBytes = srcRhk.encode();
            var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
            var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

            set(srcKeyBytes, encodedBytes, slotWithKeyHashForSrc, spType);
        }

        var dstRhk = getByKeyBytes(dstKeyBytes, slotWithKeyHashForDst);
        if (dstRhk == null) {
            dstRhk = new RedisHashKeys();
        }
        dstRhk.add(member);

        var encodedBytes = dstRhk.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

        set(dstKeyBytes, encodedBytes, slotWithKeyHashForDst, spType);
        return IntegerReply.REPLY_1;
    }

    private Reply srandmember(boolean doPop) {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        boolean hasCount = data.length == 3;
        int count = 1;
        if (hasCount) {
            var countBytes = data[2];
            try {
                count = Integer.parseInt(new String(countBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var slot = slotWithKeyHash.slot();

        var rhk = getByKeyBytes(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var set = rhk.getSet();
        if (set.isEmpty()) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = set.size();
        if (count > size) {
            count = size;
        }

        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = new ArrayList<>();
        if (count == size) {
            // need not random, return all members
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

        var members = new String[indexes.size()];

        int j = 0;
        var it = set.iterator();
        while (it.hasNext()) {
            var member = it.next();

            // only remove once
            boolean isAlreadyRemoved = false;

            for (int k = 0; k < indexes.size(); k++) {
                Integer index = indexes.get(k);
                if (index != null && index == j) {
                    members[k] = member;
                    if (!isAlreadyRemoved && doPop) {
                        it.remove();
                        isAlreadyRemoved = true;
                    }
                    indexes.set(k, null);
                }
            }
            j++;
        }

        if (doPop) {
            if (set.isEmpty()) {
                // remove key
                var oneSlot = localPersist.oneSlot(slot);
                oneSlot.removeDelay(workerId, new String(keyBytes), slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
            } else {
                var encodedBytes = rhk.encode();
                var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

                set(keyBytes, encodedBytes, slotWithKeyHash, spType);
            }
        }

        if (hasCount) {
            var replies = new Reply[members.length];
            for (int i = 0; i < members.length; i++) {
                replies[i] = new BulkReply(members[i].getBytes());
            }
            return new MultiBulkReply(replies);
        } else {
            return new BulkReply(members[0].getBytes());
        }
    }

    private Reply srem() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        byte[][] memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var slot = slotWithKeyHash.slot();

        var rhk = getByKeyBytes(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        int removed = 0;
        for (var memberBytes : memberBytesArr) {
            boolean isMember = rhk.remove(new String(memberBytes));
            if (isMember) {
                removed++;
            }
        }

        if (rhk.size() == 0) {
            // remove key
            var oneSlot = localPersist.oneSlot(slot);
            oneSlot.removeDelay(workerId, new String(keyBytes), slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
        } else {
            var encodedBytes = rhk.encode();
            var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
            var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

            set(keyBytes, encodedBytes, slotWithKeyHash, spType);
        }

        return new IntegerReply(removed);
    }
}
