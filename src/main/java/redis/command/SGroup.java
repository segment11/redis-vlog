
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.VisibleForTesting;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.Dict;
import redis.TrainSampleJob;
import redis.dyn.CachedGroovyClassLoader;
import redis.dyn.RefreshLoader;
import redis.repl.LeaderSelector;
import redis.reply.*;
import redis.type.RedisHashKeys;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static redis.CompressedValue.NO_EXPIRE;

public class SGroup extends BaseCommand {
    public SGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("set".equals(cmd) || "setex".equals(cmd) || "setrange".equals(cmd) ||
                "setnx".equals(cmd) || "strlen".equals(cmd) || "substr".equals(cmd) ||
                "sadd".equals(cmd) || "scard".equals(cmd) ||
                "sismember".equals(cmd) || "smembers".equals(cmd) || "smismember".equals(cmd) ||
                "spop".equals(cmd) || "srandmember".equals(cmd) || "srem".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("sdiff".equals(cmd) || "sinter".equals(cmd) || "sunion".equals(cmd) ||
                "sdiffstore".equals(cmd) || "sinterstore".equals(cmd) || "sunionstore".equals(cmd)) {
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

        if ("sintercard".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            for (int i = 2; i < data.length; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("smove".equals(cmd)) {
            if (data.length != 4) {
                return slotWithKeyHashList;
            }
            var srcKeyBytes = data[1];
            var dstKeyBytes = data[2];

            var s1 = slot(srcKeyBytes, slotNumber);
            var s2 = slot(dstKeyBytes, slotNumber);
            slotWithKeyHashList.add(s1);
            slotWithKeyHashList.add(s2);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("sentinel".equals(cmd)) {
            return sentinel();
        }

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
            var reply = set(dd);
            if (reply instanceof ErrorReply) {
                return reply;
            }
            return reply == OKReply.INSTANCE ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
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

        if ("subscribe".equals(cmd)) {
            return subscribe();
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

    private Reply sentinel() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/script/SentinelCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("sGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }

    private static final String IPV4_REGEX =
            "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";

    private static final Pattern IPv4_PATTERN = Pattern.compile(IPV4_REGEX);

    @VisibleForTesting
    Reply slaveof() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var hostBytes = data[1];
        var portBytes = data[2];

        var leaderSelector = LeaderSelector.getInstance();

        var isNoOne = "no".equalsIgnoreCase(new String(hostBytes));
        if (isNoOne) {
            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            leaderSelector.resetAsMaster(true, (e) -> {
                if (e != null) {
                    log.error("slaveof error: {}", e.getMessage());
                    finalPromise.set(new ErrorReply(e.getMessage()));
                    return;
                }

                finalPromise.set(OKReply.INSTANCE);
            });

            return asyncReply;
        }

        var host = new String(hostBytes);
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
        if (port < 0 || port > 65535) {
            return ErrorReply.INVALID_INTEGER;
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        leaderSelector.resetAsSlave(true, host, port, (e) -> {
            if (e != null) {
                log.error("slaveof error: {}", e.getMessage());
                finalPromise.set(new ErrorReply(e.getMessage()));
                return;
            }

            finalPromise.set(OKReply.INSTANCE);
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply set(byte[][] dd) {
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
            } else {
//            } else if (isPxAt) {
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

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
            if (isOldExist && isReturnExist) {
                if (!cv.isTypeString()) {
                    log.debug("Key {} is not string type", new String(keyBytes));
                    return ErrorReply.NOT_STRING;
                }
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
                return new BulkReply(getValueBytesByCv(cv));
            }
        }

        return OKReply.INSTANCE;
    }

    @VisibleForTesting
    Reply setrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var offsetBytes = data[2];
        var valueBytes = data[3];

        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
            return ErrorReply.VALUE_TOO_LONG;
        }

        int offset;
        try {
            offset = Integer.parseInt(new String(offsetBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }
        if (offset < 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();

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

    @VisibleForTesting
    Reply strlen() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }
        return new IntegerReply(cv.getUncompressedLength());
    }

    @VisibleForTesting
    Reply select() {
        return ErrorReply.NOT_SUPPORT;
    }

    private RedisHashKeys getRedisSet(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_SET);
        if (encodedBytes == null) {
            return null;
        }

        return RedisHashKeys.decode(encodedBytes);
    }

    private void saveRedisSet(RedisHashKeys rhk, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var key = new String(keyBytes);
        if (rhk.size() == 0) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(keyBytes, rhk.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_SET);
    }

    @VisibleForTesting
    Reply sadd() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        // use RedisHashKeys to store set
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            rhk = new RedisHashKeys();
        }

        int added = 0;
        for (var memberBytes : memberBytesArr) {
            boolean isNewAdded = rhk.add(new String(memberBytes));
            if (rhk.size() > RedisHashKeys.HASH_MAX_SIZE) {
                return ErrorReply.SET_SIZE_TO_LONG;
            }
            if (isNewAdded) {
                added++;
            }
        }

        saveRedisSet(rhk, keyBytes, slotWithKeyHash);
        return new IntegerReply(added);
    }

    @VisibleForTesting
    Reply scard() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_SET);
        if (encodedBytes == null) {
            return IntegerReply.REPLY_0;
        }

        var size = RedisHashKeys.getSizeWithoutDecode(encodedBytes);
        return new IntegerReply(size);
    }

    @VisibleForTesting
    private void operateSet(TreeSet<String> set, ArrayList<RedisHashKeys> otherRhkList, boolean isInter, boolean isUnion) {
        for (var otherRhk : otherRhkList) {
            if (otherRhk != null) {
                var otherSet = otherRhk.getSet();
                if (isInter) {
                    if (otherSet.isEmpty()) {
                        set.clear();
                        break;
                    }
                    set.retainAll(otherSet);
                } else if (isUnion) {
                    set.addAll(otherSet);
                } else {
                    // diff
                    set.removeAll(otherSet);
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
    }

    @VisibleForTesting
    Reply sdiff(boolean isInter, boolean isUnion) {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        ArrayList<SlotWithKeyHashWithKeyBytes> list = new ArrayList<>(data.length - 1);
        for (int i = 1, j = 0; i < data.length; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new SlotWithKeyHashWithKeyBytes(slotWithKeyHash, data[i]));
        }

        var first = list.getFirst();
        var rhk = getRedisSet(first.keyBytes(), first.slotWithKeyHash());
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rhk.size() == 0) {
            if (isInter) {
                return MultiBulkReply.EMPTY;
            }
            if (!isUnion) {
                return MultiBulkReply.EMPTY;
            }
        }

        var set = rhk.getSet();
        if (!isCrossRequestWorker) {
            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (int i = 1; i < list.size(); i++) {
                var other = list.get(i);
                var otherRhk = getRedisSet(other.keyBytes(), other.slotWithKeyHash());
                otherRhkList.add(otherRhk);
            }
            operateSet(set, otherRhkList, isInter, isUnion);

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

        ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size() - 1);
        for (int i = 1; i < list.size(); i++) {
            var other = list.get(i);
            var otherSlotWithKeyHash = other.slotWithKeyHash();
            var otherKeyBytes = other.keyBytes();

            var oneSlot = localPersist.oneSlot(otherSlotWithKeyHash.slot());
            var p = oneSlot.asyncCall(() -> getRedisSet(otherKeyBytes, otherSlotWithKeyHash));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // need not wait all, can optimize
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("sdiff error: {}, isInter: {}, isUnion: {}", e.getMessage(), isInter, isUnion);
                finalPromise.setException(e);
                return;
            }

            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (var promise : promises) {
                otherRhkList.add(promise.getResult());
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            if (set.isEmpty()) {
                finalPromise.set(MultiBulkReply.EMPTY);
                return;
            }

            var replies = new Reply[set.size()];
            int i = 0;
            for (var value : set) {
                replies[i++] = new BulkReply(value.getBytes());
            }
            finalPromise.set(new MultiBulkReply(replies));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply sdiffstore(boolean isInter, boolean isUnion) {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var dstKeyBytes = data[1];
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();

        ArrayList<SlotWithKeyHashWithKeyBytes> list = new ArrayList<>(data.length - 2);
        // begin from 2
        // j = 1 -> dst key bytes is 0
        for (int i = 2, j = 1; i < data.length; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new SlotWithKeyHashWithKeyBytes(slotWithKeyHash, data[i]));
        }

        if (!isCrossRequestWorker) {
            // first key may be in other thread eventloop
            var first = list.getFirst();
            var rhk = getRedisSet(first.keyBytes(), first.slotWithKeyHash());
            if (rhk == null) {
                removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                return IntegerReply.REPLY_0;
            }
            if (rhk.size() == 0) {
                if (isInter) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    return IntegerReply.REPLY_0;
                }
                if (!isUnion) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    return IntegerReply.REPLY_0;
                }
            }

            var set = rhk.getSet();

            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (int i = 1; i < list.size(); i++) {
                var other = list.get(i);
                var otherRhk = getRedisSet(other.keyBytes(), other.slotWithKeyHash());
                otherRhkList.add(otherRhk);
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            saveRedisSet(rhk, dstKeyBytes, dstSlotWithKeyHash);
            return set.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(set.size());
        }

        ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            var other = list.get(i);
            var otherSlotWithKeyHash = other.slotWithKeyHash();
            var otherKeyBytes = other.keyBytes();

            var oneSlot = localPersist.oneSlot(otherSlotWithKeyHash.slot());
            var p = oneSlot.asyncCall(() -> getRedisSet(otherKeyBytes, otherSlotWithKeyHash));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // need not wait all, can optimize
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("sdiffstore error: {}, isInter: {}, isUnion: {}", e.getMessage(), isInter, isUnion);
                finalPromise.setException(e);
                return;
            }

            var rhk = promises.getFirst().getResult();
            if (rhk == null) {
                removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                finalPromise.set(IntegerReply.REPLY_0);
                return;
            }
            if (rhk.size() == 0) {
                if (isInter) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }
                if (!isUnion) {
                    removeDelay(dstSlotWithKeyHash.slot(), dstSlotWithKeyHash.bucketIndex(), new String(dstKeyBytes), dstSlotWithKeyHash.keyHash());
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }
            }

            var set = rhk.getSet();

            ArrayList<RedisHashKeys> otherRhkList = new ArrayList<>(list.size() - 1);
            for (var promise : promises) {
                otherRhkList.add(promise.getResult());
            }
            operateSet(set, otherRhkList, isInter, isUnion);

            saveRedisSet(rhk, dstKeyBytes, dstSlotWithKeyHash);
            finalPromise.set(set.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(set.size()));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply sintercard() {
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

        ArrayList<SlotWithKeyHashWithKeyBytes> list = new ArrayList<>(numkeys);
        // begin from 2
        for (int i = 2, j = 0; i < numkeys + 2; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var slotWithKeyHash = slotWithKeyHashListParsed.get(j);
            list.add(new SlotWithKeyHashWithKeyBytes(slotWithKeyHash, data[i]));
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

        var first = list.getFirst();
        var rhk = getRedisSet(first.keyBytes(), first.slotWithKeyHash());
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }
        if (rhk.size() == 0) {
            return IntegerReply.REPLY_0;
        }

        var set = rhk.getSet();
        if (!isCrossRequestWorker) {
            for (int i = 1; i < list.size(); i++) {
                var other = list.get(i);
                var otherRhk = getRedisSet(other.keyBytes(), other.slotWithKeyHash());

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
            return min == 0 ? IntegerReply.REPLY_0 : new IntegerReply(min);
        }

        ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>(list.size() - 1);
        for (int i = 1; i < list.size(); i++) {
            var other = list.get(i);
            var otherSlotWithKeyHash = other.slotWithKeyHash();
            var otherKeyBytes = other.keyBytes();

            var oneSlot = localPersist.oneSlot(otherSlotWithKeyHash.slot());
            var p = oneSlot.asyncCall(() -> getRedisSet(otherKeyBytes, otherSlotWithKeyHash));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        // need not wait all, can optimize
        int finalLimit = limit;
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("sintercard error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            for (var promise : promises) {
                var otherRhk = promise.getResult();
                if (otherRhk != null) {
                    set.retainAll(otherRhk.getSet());
                    if (set.isEmpty()) {
                        break;
                    }
                    if (finalLimit != 0 && set.size() >= finalLimit) {
                        break;
                    }
                } else {
                    set.clear();
                    break;
                }
            }

            int min = finalLimit != 0 ? Math.min(set.size(), finalLimit) : set.size();
            finalPromise.set(min == 0 ? IntegerReply.REPLY_0 : new IntegerReply(min));
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply sismember() {
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }

        var isMember = rhk.contains(new String(memberBytes));
        return isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
    }

    @VisibleForTesting
    Reply smembers() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rhk.size() == 0) {
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

    @VisibleForTesting
    Reply smismember() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rhk.size() == 0) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[memberBytesArr.length];
        for (int i = 0; i < memberBytesArr.length; i++) {
            var isMember = rhk.contains(new String(memberBytesArr[i]));
            replies[i] = isMember ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply smove() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        var dstKeyBytes = data[2];

        if (srcKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[3];
        if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
            return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
        }

        var srcSlotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();

        var srcRhk = getRedisSet(srcKeyBytes, srcSlotWithKeyHash);
        if (srcRhk == null) {
            return IntegerReply.REPLY_0;
        }

        var member = new String(memberBytes);
        var isMember = srcRhk.remove(member);
        if (!isMember) {
            return IntegerReply.REPLY_0;
        }

        saveRedisSet(srcRhk, srcKeyBytes, srcSlotWithKeyHash);

        if (!isCrossRequestWorker) {
            var dstRhk = getRedisSet(dstKeyBytes, dstSlotWithKeyHash);
            if (dstRhk == null) {
                dstRhk = new RedisHashKeys();
            }
            dstRhk.add(member);

            saveRedisSet(dstRhk, dstKeyBytes, dstSlotWithKeyHash);
            return IntegerReply.REPLY_1;
        }

        var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        dstOneSlot.asyncRun(() -> {
            var dstRhk = getRedisSet(dstKeyBytes, dstSlotWithKeyHash);
            if (dstRhk == null) {
                dstRhk = new RedisHashKeys();
            }
            dstRhk.add(member);

            saveRedisSet(dstRhk, dstKeyBytes, dstSlotWithKeyHash);
            finalPromise.set(IntegerReply.REPLY_1);
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply srandmember(boolean doPop) {
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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }
        if (rhk.size() == 0) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var set = rhk.getSet();
        int size = set.size();
        if (count > size) {
            count = size;
        }

        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = HGroup.getRandIndex(count, size, absCount);

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
            saveRedisSet(rhk, keyBytes, slotWithKeyHash);
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

    @VisibleForTesting
    Reply srem() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 2; i < data.length; i++) {
            var memberBytes = data[i];
            if (memberBytes.length > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                return ErrorReply.SET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i - 2] = memberBytes;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rhk = getRedisSet(keyBytes, slotWithKeyHash);
        if (rhk == null) {
            return IntegerReply.REPLY_0;
        }
        if (rhk.size() == 0) {
            return IntegerReply.REPLY_0;
        }

        int removed = 0;
        for (var memberBytes : memberBytesArr) {
            var isMember = rhk.remove(new String(memberBytes));
            if (isMember) {
                removed++;
            }
        }

        saveRedisSet(rhk, keyBytes, slotWithKeyHash);
        return new IntegerReply(removed);
    }

    @VisibleForTesting
    Reply subscribe() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var channels = new ArrayList<String>(data.length - 1);
        for (int i = 1; i < data.length; i++) {
            var channel = new String(data[i]);
            channels.add(channel);
        }

        var socketInInspector = localPersist.getSocketInspector();

        var replies = new Reply[channels.size() * 3];
        int j = 0;
        for (var channel : channels) {
            replies[j++] = new BulkReply("subscribe".getBytes());
            replies[j++] = new BulkReply(channel.getBytes());
            var size = socketInInspector.subscribe(channel, socket);
            replies[j++] = new IntegerReply(size);
        }

        return new MultiBulkReply(replies);
    }
}
