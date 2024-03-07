
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.reply.BulkReply;
import redis.reply.ErrorReply;
import redis.reply.NilReply;
import redis.reply.Reply;

import static redis.CompressedValue.NO_EXPIRE;

public class GGroup extends BaseCommand {
    public GGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("get".equals(cmd) || "getdel".equals(cmd) || "getex".equals(cmd)
                || "getrange".equals(cmd) || "getset".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("getdel".equals(cmd)) {
            return getdel();
        }

        if ("getex".equals(cmd)) {
            return getex();
        }

        if ("getrange".equals(cmd)) {
            return getrange();
        }

        if ("getset".equals(cmd)) {
            return getset();
        }

        return NilReply.INSTANCE;
    }

    private Reply getdel() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var slot = slotWithKeyHash.slot();
        var key = new String(keyBytes);

        var valueBytes = get(keyBytes, slotWithKeyHash);
        if (valueBytes != null) {
            var oneSlot = localPersist.oneSlot(slot);
            oneSlot.removeDelay(workerId, key, slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
            return new BulkReply(valueBytes);
        } else {
            return NilReply.INSTANCE;
        }
    }

    private Reply getex() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        long ex = -1;
        long px = -1;
        long exAt = -1;
        long pxAt = -1;
        boolean isPersist;

        if (data.length > 2) {
            var arg = new String(data[2]);
            if ("persist".equalsIgnoreCase(arg)) {
                isPersist = true;
            } else {
                isPersist = false;
                if (data.length != 4) {
                    return ErrorReply.FORMAT;
                }

                var arg2 = new String(data[3]);
                long x;
                try {
                    x = Long.parseLong(arg2);
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                if (x < 0) {
                    return ErrorReply.INVALID_INTEGER;
                }

                if ("ex".equalsIgnoreCase(arg)) {
                    ex = x;
                } else if ("px".equalsIgnoreCase(arg)) {
                    px = x;
                } else if ("exat".equalsIgnoreCase(arg)) {
                    exAt = x;
                } else if ("pxat".equalsIgnoreCase(arg)) {
                    pxAt = x;
                } else {
                    return ErrorReply.SYNTAX;
                }
            }
        } else {
            isPersist = false;
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);

        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return NilReply.INSTANCE;
        }

        var valueBytes = getValueBytesByCv(cv);

        long expireAt = cv.getExpireAt();
        if (isPersist) {
            expireAt = NO_EXPIRE;
        } else if (ex > -1) {
            expireAt = System.currentTimeMillis() + ex * 1000;
        } else if (px > -1) {
            expireAt = System.currentTimeMillis() + px;
        } else if (exAt > -1) {
            expireAt = exAt * 1000;
        } else if (pxAt > -1) {
            expireAt = pxAt;
        }
        cv.setExpireAt(expireAt);

        setCv(keyBytes, cv, slotWithKeyHash);
        return new BulkReply(valueBytes);
    }

    final static Reply BLANK_REPLY = new BulkReply(new byte[0]);

    Reply getrange() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var startBytes = data[2];
        var endBytes = data[3];

        int start;
        int end;
        try {
            start = Integer.parseInt(new String(startBytes));
            end = Integer.parseInt(new String(endBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        var valueBytes = get(keyBytes, slotPreferParsed(keyBytes));
        if (valueBytes == null) {
            return NilReply.INSTANCE;
        }

        if (start < 0) {
            start = valueBytes.length + start;
            if (start < 0) {
                start = 0;
            }
        }
        if (end < 0) {
            end = valueBytes.length + end;
            if (end < 0) {
                return BLANK_REPLY;
            }
        }
        if (start >= valueBytes.length) {
            return BLANK_REPLY;
        }
        if (end >= valueBytes.length) {
            end = valueBytes.length - 1;
        }
        if (start > end) {
            return BLANK_REPLY;
        }

        // use utf-8 ? or use bytes
//        var value = new String(valueBytes);

        var subBytes = new byte[end - start + 1];
        System.arraycopy(valueBytes, start, subBytes, 0, subBytes.length);
        return new BulkReply(subBytes);
    }

    private Reply getset() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var valueBytes = data[2];

        var slotWithKeyHash = slotPreferParsed(keyBytes);

        var valueBytesExist = get(keyBytes, slotWithKeyHash);
        if (valueBytesExist == null) {
            return NilReply.INSTANCE;
        }

        set(keyBytes, valueBytes, slotWithKeyHash);
        return new BulkReply(valueBytesExist);
    }
}
