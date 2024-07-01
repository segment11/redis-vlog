
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.reply.*;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static redis.CompressedValue.NO_EXPIRE;

public class EGroup extends BaseCommand {
    public EGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("exists".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }

            for (int i = 1; i < data.length; i++) {
                var keyBytes = data[i];
                slotWithKeyHashList.add(slot(keyBytes, slotNumber));
            }

            return slotWithKeyHashList;
        }

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

        if (!isCrossRequestWorker) {
            int n = 0;
            for (int i = 1; i < data.length; i++) {
                var keyBytes = data[i];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;
                }
                var key = new String(keyBytes);

                var slotWithKeyHash = slotWithKeyHashListParsed.get(i - 1);
                var slot = slotWithKeyHash.slot();
                var bucketIndex = slotWithKeyHash.bucketIndex();
                var keyHash = slotWithKeyHash.keyHash();

                // remove delay, perf better
                var isExists = exists(slot, bucketIndex, key, keyHash);
                if (isExists) {
                    n++;
                }
            }
            return new IntegerReply(n);
        }

        ArrayList<DGroup.SlotWithKeyHashWithKeyBytes> list = new ArrayList<>(data.length - 1);
        for (int i = 1; i < data.length; i++) {
            list.add(new DGroup.SlotWithKeyHashWithKeyBytes(slotWithKeyHashListParsed.get(i - 1), data[i]));
        }

        ArrayList<Promise<ArrayList<Boolean>>> promises = new ArrayList<>();
        // group by slot
        var groupBySlot = list.stream().collect(Collectors.groupingBy(it -> it.slotWithKeyHash().slot()));
        for (var entry : groupBySlot.entrySet()) {
            var slot = entry.getKey();
            var subList = entry.getValue();

            var oneSlot = localPersist.oneSlot(slot);
            var p = oneSlot.asyncCall(() -> {
                ArrayList<Boolean> valueList = new ArrayList<>();
                for (var one : subList) {
                    var key = new String(one.keyBytes());
                    var bucketIndex = one.slotWithKeyHash().bucketIndex();
                    var keyHash = one.slotWithKeyHash().keyHash();

                    var isExists = exists(oneSlot.slot(), bucketIndex, key, keyHash);
                    valueList.add(isExists);
                }
                return valueList;
            });
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("exists error: {}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            int n = 0;
            for (var p : promises) {
                for (var b : p.getResult()) {
                    if (b) {
                        n++;
                    }
                }
            }

            finalPromise.set(new IntegerReply(n));
        });

        return asyncReply;
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
        if (isGt && expireAtExist != NO_EXPIRE && expireAtExist >= expireAt) {
            return IntegerReply.REPLY_0;
        }
        if (isLt && expireAtExist != NO_EXPIRE && expireAtExist <= expireAt) {
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
