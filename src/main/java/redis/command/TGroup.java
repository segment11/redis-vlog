
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.*;
import redis.type.RedisHashKeys;

import static redis.CompressedValue.NO_EXPIRE;

public class TGroup extends BaseCommand {
    static final BulkReply TYPE_STRING = new BulkReply("string".getBytes());
    static final BulkReply TYPE_HASH = new BulkReply("hash".getBytes());
    static final BulkReply TYPE_LIST = new BulkReply("list".getBytes());
    static final BulkReply TYPE_SET = new BulkReply("set".getBytes());
    static final BulkReply TYPE_ZSET = new BulkReply("zset".getBytes());
    static final BulkReply TYPE_STREAM = new BulkReply("stream".getBytes());

    public TGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("type".equals(cmd) || "ttl".equals(cmd)) {
            if (data.length != 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }
        
        return null;
    }

    public Reply handle() {
        if ("type".equals(cmd)) {
            return type();
        }

        if ("ttl".equals(cmd)) {
            return ttl(false);
        }

        return NilReply.INSTANCE;
    }

    private Reply type() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        // need not decompress at all, todo: optimize
        var cv = getCv(keyBytes, slotPreferParsed(keyBytes));
        if (cv == null) {
            // hash keys changed
            var keysKey = RedisHashKeys.keysKey(new String(keyBytes));
            cv = getCv(keysKey.getBytes());

            if (cv == null) {
                return NilReply.INSTANCE;
            }
        }

        if (cv.isHash()) {
            return TYPE_HASH;
        }
        if (cv.isList()) {
            return TYPE_LIST;
        }
        if (cv.isSet()) {
            return TYPE_SET;
        }
        if (cv.isZSet()) {
            return TYPE_ZSET;
        }
        if (cv.isStream()) {
            return TYPE_STREAM;
        }

        return TYPE_STRING;
    }

    Reply ttl(boolean isMilliseconds) {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var cv = getCv(keyBytes, slotPreferParsed(keyBytes));
        if (cv == null) {
            return new IntegerReply(-2);
        }

        var expireAt = cv.getExpireAt();
        if (expireAt == NO_EXPIRE) {
            return new IntegerReply(-1);
        }

        var ttlMilliseconds = expireAt - System.currentTimeMillis();
        return new IntegerReply(isMilliseconds ? ttlMilliseconds : ttlMilliseconds / 1000);
    }
}
