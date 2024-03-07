package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.netty.buffer.Unpooled;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.Debug;
import redis.reply.*;
import redis.type.RedisHashKeys;
import redis.type.RedisList;

import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;
import static redis.CompressedValue.NO_EXPIRE;
import static redis.CompressedValue.NULL_DICT_SEQ;

public class RGroup extends BaseCommand {
    public RGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("rpoplpush".equals(cmd) || "rename".equals(cmd)) {
            if (data.length != 3) {
                return null;
            }
            var dstKeyBytes = data[2];
            return slot(dstKeyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("rename".equals(cmd)) {
            return rename();
        }

        if ("restore".equals(cmd)) {
            return restore();
        }

        if ("rpop".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpop(false);
        }

        if ("rpoplpush".equals(cmd)) {
            return rpoplpush();
        }

        if ("rpush".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpush(false, false);
        }

        if ("rpushx".equals(cmd)) {
            var lGroup = new LGroup(cmd, data, socket);
            lGroup.from(this);
            return lGroup.lpush(false, true);
        }

        return NilReply.INSTANCE;
    }

    private Reply rename() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        var dstKeyBytes = data[2];

        var slotWithKeyHash = slot(srcKeyBytes);
        var srcCv = getCv(srcKeyBytes, slotWithKeyHash);
        if (srcCv == null) {
            return ErrorReply.NO_SUCH_KEY;
        }

        // key mask value will be overwritten by setCv
        var dstSlotWithKeyHash = slot(dstKeyBytes);
        var dstSlot = dstSlotWithKeyHash.slot();

        setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
//        var oneSlot = localPersist.oneSlot(dstSlot);
//        var dstBucketIndex = oneSlot.bucketIndex(dstSlotWithKeyHash.keyHash());
//        keyLoader.bucketLock(dstSlot, dstBucketIndex, () -> setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash));

        var srcKey = new String(srcKeyBytes);
        var srcSlot = slotWithKeyHash.slot();

        var oneSlotSrc = localPersist.oneSlot(srcSlot);
        oneSlotSrc.removeDelay(workerId, srcKey, slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
//        var srcBucketIndex = oneSlotSrc.bucketIndex(slotWithKeyHash.keyHash());
//        keyLoader.bucketLock(srcSlot, srcBucketIndex, () -> oneSlotSrc.removeDelay(workerId, srcKey, slotWithKeyHash.keyHash()));
        return OKReply.INSTANCE;
    }

    final static String REPLACE = "replace";
    final static String ABSTTL = "absttl";
    final static String IDLETIME = "idletime";
    final static String FREQ = "freq";

    private Reply restore() {
        // refer kvrocks: cmd_server.cc redis::CommandRestore
        var keyBytes = data[1];
        var ttlBytes = data[2];
        var serializedValue = data[3];

        var key = new String(keyBytes);
        var ttl = new String(ttlBytes);

        // for debug
        if (Debug.getInstance().logRestore) {
            log.info("key: {}", key);
            log.info("ttl: {}", ttl);
        }

        boolean replace = false;
        boolean absttl = false;
        int idleTime = 0;
        int freq = 0;
        if (data.length > 4) {
            for (int i = 4; i < data.length; i++) {
                var arg = new String(data[i]);
                if (REPLACE.equalsIgnoreCase(arg)) {
                    replace = true;
                } else if (ABSTTL.equalsIgnoreCase(arg)) {
                    absttl = true;
                } else if (IDLETIME.equalsIgnoreCase(arg)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    idleTime = Integer.parseInt(new String(data[i + 1]));
                    if (idleTime < 0) {
                        return new ErrorReply("IDLETIME can't be negative");
                    }
                } else if (FREQ.equalsIgnoreCase(arg)) {
                    if (data.length <= i + 1) {
                        return ErrorReply.SYNTAX;
                    }
                    freq = Integer.parseInt(new String(data[i + 1]));
                    if (freq < 0 || freq > 255) {
                        return new ErrorReply("FREQ must be in range 0..255");
                    }
                }
            }
        }

        long expireAt;
        if (ttlBytes.length == 0 || ttl.equals("0")) {
            // -1 means no expire, different from redis, important!!!
            expireAt = NO_EXPIRE;
        } else {
            if (absttl) {
                expireAt = Long.parseLong(ttl);
            } else {
                expireAt = System.currentTimeMillis() + Long.parseLong(ttl);
            }
        }

        var slotWithKeyHash = slot(keyBytes);
        // check if key exists
        replace = true;
        if (!replace) {
            var cv = getCv(keyBytes, slotWithKeyHash);
            if (cv != null) {
                return new ErrorReply("Target key name is busy.");
            }
        }

        var buf = Unpooled.wrappedBuffer(serializedValue);
        var rdb = new RDB(buf);
        try {
            final long finalExpireAt = expireAt;

            rdb.restore(new RDB.Callback() {
                @Override
                public void onInteger(Integer value) {
                    setNumber(keyBytes, value, slotWithKeyHash, finalExpireAt);
                }

                @Override
                public void onString(byte[] valueBytes) {
                    set(keyBytes, valueBytes, slotWithKeyHash, NULL_DICT_SEQ, finalExpireAt);
                }

                @Override
                public void onList(byte[] valueBytes) {
                    var needCompress = valueBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                    var spType = needCompress ? CompressedValue.SP_TYPE_LIST_COMPRESSED : CompressedValue.SP_TYPE_LIST;

                    set(keyBytes, valueBytes, slotWithKeyHash, spType, finalExpireAt);
                }

                @Override
                public void onSet(byte[] encodedBytes) {
                    var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                    var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

                    set(keyBytes, encodedBytes, slotWithKeyHash, spType, finalExpireAt);
                }

                @Override
                public void onZSet(byte[] encodedBytes) {
                    var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                    var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

                    set(keyBytes, encodedBytes, slotWithKeyHash, spType, finalExpireAt);
                }

                @Override
                public void onHashKeys(byte[] encodedBytes) {
                    var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                    var spType = needCompress ? CompressedValue.SP_TYPE_HASH_COMPRESSED : CompressedValue.SP_TYPE_HASH;

                    var keysKey = RedisHashKeys.keysKey(key);
                    var keysKeyBytes = keysKey.getBytes();

                    set(keysKeyBytes, encodedBytes, null, spType, finalExpireAt);
                }

                @Override
                public void onHashFieldValues(String field, byte[] fieldValueBytes) {
                    var fieldKey = RedisHashKeys.fieldKey(key, field);
                    set(fieldKey.getBytes(), fieldValueBytes);
                }
            });
            return OKReply.INSTANCE;
        } catch (Exception e) {
            log.error("Restore error", e);
            return new ErrorReply(e.getMessage());
        }
    }

    byte[] move(byte[] srcKeyBytes, byte[] dstKeyBytes, boolean srcLeft, boolean dstLeft) {
        var srcSlotWithKeyHash = slot(srcKeyBytes);
        var dstSlotWithKeyHash = slotPreferParsed(dstKeyBytes);

        var cv = getCv(srcKeyBytes, srcSlotWithKeyHash);
        if (cv == null) {
            return null;
        }
        if (!cv.isList()) {
            return null;
        }

        var encodedBytesExist = getValueBytesByCv(cv);
        var rl = RedisList.decode(encodedBytesExist);

        var size = rl.size();
        if (size == 0) {
            return null;
        }

        var e = srcLeft ? rl.removeFirst() : rl.removeLast();
        var cvDst = getCv(dstKeyBytes, dstSlotWithKeyHash);

        RedisList rlDst;
        if (cvDst == null) {
            rlDst = new RedisList();
        } else {
            if (!cvDst.isList()) {
                return null;
            }

            var encodedBytesDst = getValueBytesByCv(cvDst);
            rlDst = RedisList.decode(encodedBytesDst);
        }

        if (dstLeft) {
            rlDst.addFirst(e);
        } else {
            rlDst.addLast(e);
        }

        var encodedBytesDst = rlDst.encode();
        var needCompressDst = encodedBytesDst.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spTypeDst = needCompressDst ? CompressedValue.SP_TYPE_LIST_COMPRESSED : CompressedValue.SP_TYPE_LIST;

        set(dstKeyBytes, encodedBytesDst, dstSlotWithKeyHash, spTypeDst);

        var encodedBytes = rl.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_LIST_COMPRESSED : CompressedValue.SP_TYPE_LIST;

        set(srcKeyBytes, encodedBytes, srcSlotWithKeyHash, spType);
        return e;
    }

    private Reply rpoplpush() {
        if (data.length != 3) {
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

        var valueBytes = move(srcKeyBytes, dstKeyBytes, false, true);
        if (valueBytes == null) {
            return NilReply.INSTANCE;
        }
        return new BulkReply(valueBytes);
    }
}
