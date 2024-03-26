
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.reply.*;

import java.util.ArrayList;

import static redis.CompressedValue.NO_EXPIRE;

public class EGroup extends BaseCommand {
    public EGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("expire".equals(cmd) || "expireat".equals(cmd) || "expiretime".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("exists".equals(cmd)) {
            return exists();
        }

        if ("expire".equals(cmd)) {
            return expire(false, false);
        }

        if ("expireat".equals(cmd)) {
            return expire(true, false);
        }

        if ("expiretime".equals(cmd)) {
            return expiretime(false);
        }

        if ("echo".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }
            return new BulkReply(data[1]);
        }

        return NilReply.INSTANCE;
    }

    private Reply exists() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        // group by bucket index better, refer to mget, todo

        int n = 0;
        for (int i = 1; i < data.length; i++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var cv = getCv(keyBytes);
            if (cv != null) {
                n++;
            }
        }
        return new IntegerReply(n);
    }

    Reply expire(boolean isAt, boolean isMilliseconds) {
        if (data.length != 3 && data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var secondsBytes = data[2];

        long seconds;
        try {
            seconds = Long.parseLong(new String(secondsBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        long expireAt;
        if (isMilliseconds) {
            expireAt = isAt ? seconds : System.currentTimeMillis() + seconds;
        } else {
            expireAt = isAt ? seconds * 1000 : System.currentTimeMillis() + seconds * 1000;
        }

        boolean isNx = false;
        boolean isXx = false;
        boolean isGt = false;
        boolean isLt = false;

        if (data.length == 4) {
            var typeBytes = data[3];
            var type = new String(typeBytes);
            isNx = "nx".equalsIgnoreCase(type);
            isXx = "xx".equalsIgnoreCase(type);
            isGt = "gt".equalsIgnoreCase(type);
            isLt = "lt".equalsIgnoreCase(type);
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return IntegerReply.REPLY_0;
        }

        var expireAtExist = cv.getExpireAt();
        if (isNx && expireAtExist != NO_EXPIRE) {
            return IntegerReply.REPLY_0;
        }
        if (isXx && expireAtExist == NO_EXPIRE) {
            return IntegerReply.REPLY_0;
        }
        if (isGt && expireAtExist != NO_EXPIRE && expireAtExist <= expireAt) {
            return IntegerReply.REPLY_0;
        }
        if (isLt && expireAtExist != NO_EXPIRE && expireAtExist >= expireAt) {
            return IntegerReply.REPLY_0;
        }

        cv.setSeq(snowFlake.nextId());
        cv.setExpireAt(expireAt);

        setCv(keyBytes, cv, slotWithKeyHash);
        return IntegerReply.REPLY_1;
    }

    Reply expiretime(boolean isMilliseconds) {
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

        return new IntegerReply(isMilliseconds ? expireAt : expireAt / 1000);
    }
}
