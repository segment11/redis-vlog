
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.reply.*;
import redis.type.RedisHashKeys;
import redis.type.RedisZSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public class ZGroup extends BaseCommand {
    public ZGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("zadd".equals(cmd) || "zcard".equals(cmd) || "zcount".equals(cmd)
                || "zdiffstore".equals(cmd)
                || "zincrby".equals(cmd)
                || "zinter".equals(cmd) || "zinterstore".equals(cmd)
                || "zlexcount".equals(cmd) || "zmscore".equals(cmd)
                || "zpopmax".equals(cmd) || "zpopmin".equals(cmd)
                || "zrandmember".equals(cmd)
                || "zrange".equals(cmd) || "zrangebylex".equals(cmd) || "zrangebyscore".equals(cmd)
                || "zrangestore".equals(cmd) || "zrank".equals(cmd)
                || "zrem".equals(cmd) || "zremrangebylex".equals(cmd) || "zremrangebyrank".equals(cmd) || "zremrangebyscore".equals(cmd)
                || "zrevrange".equals(cmd) || "zrevrangebylex".equals(cmd) || "zrevrangebyscore".equals(cmd) || "zrevrank".equals(cmd)
                || "zscore".equals(cmd) || "zunion".equals(cmd) || "zunionstore".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("zdiff".equals(cmd) || "zintercard".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[2];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        // sorted set group
        // like set group in SGroup
        if ("zadd".equals(cmd)) {
            return zadd();
        }

        if ("zcard".equals(cmd)) {
            return zcard();
        }

        if ("zcount".equals(cmd)) {
            return zcount(false);
        }

        if ("zdiff".equals(cmd)) {
            return zdiff();
        }

        if ("zdiffstore".equals(cmd)) {
            return zdiffstore();
        }

        if ("zincrby".equals(cmd)) {
            return zincrby();
        }

        if ("zinter".equals(cmd)) {
            return zinter(data, false, null, false);
        }

        if ("zintercard".equals(cmd)) {
            return zintercard();
        }

        if ("zinterstore".equals(cmd)) {
            if (data.length < 5) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length - 1][];
            dd[0] = data[0];
            dd[1] = data[2];
            dd[2] = data[3];
            dd[3] = data[4];

            for (int i = 5; i < data.length; i++) {
                dd[i - 1] = data[i];
            }

            return zinter(dd, true, data[1], false);
        }

        if ("zlexcount".equals(cmd)) {
            return zcount(true);
        }

        if ("zmscore".equals(cmd)) {
            return zmscore();
        }

        if ("zpopmax".equals(cmd)) {
            return zpopmax(false);
        }

        if ("zpopmin".equals(cmd)) {
            return zpopmax(true);
        }

        if ("zrandmember".equals(cmd)) {
            return zrandmember();
        }

        if ("zrange".equals(cmd)) {
            return zrange(data, false, null);
        }

        if ("zrangebylex".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 1][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "bylex".getBytes();
            for (int i = 4; i < data.length; i++) {
                dd[i + 1] = data[i];
            }

            return zrange(dd, false, null);
        }

        if ("zrangebyscore".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 1][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "byscore".getBytes();
            for (int i = 4; i < data.length; i++) {
                dd[i + 1] = data[i];
            }

            return zrange(dd, false, null);
        }

        if ("zrangestore".equals(cmd)) {
            if (data.length < 5) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length - 1][];
            dd[0] = data[0];
            dd[1] = data[2];
            dd[2] = data[3];
            dd[3] = data[4];

            for (int i = 5; i < data.length; i++) {
                dd[i - 1] = data[i];
            }

            return zrange(dd, true, data[1]);
        }

        if ("zrank".equals(cmd)) {
            return zrank(false);
        }

        if ("zrem".equals(cmd)) {
            return zrem();
        }

        if ("zremrangebylex".equals(cmd)) {
            return zremrangebyscore(false, true, false);
        }

        if ("zremrangebyrank".equals(cmd)) {
            return zremrangebyscore(false, false, true);
        }

        if ("zremrangebyscore".equals(cmd)) {
            return zremrangebyscore(true, false, false);
        }

        if ("zrevrange".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 1][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "rev".getBytes();
            for (int i = 4; i < data.length; i++) {
                dd[i + 1] = data[i];
            }

            return zrange(dd, false, null);
        }

        if ("zrevrangebylex".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 2][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "rev".getBytes();
            dd[5] = "bylex".getBytes();
            for (int i = 4; i < data.length; i++) {
                dd[i + 2] = data[i];
            }

            return zrange(dd, false, null);
        }

        if ("zrevrangebyscore".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 2][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "rev".getBytes();
            dd[5] = "byscore".getBytes();
            for (int i = 4; i < data.length; i++) {
                dd[i + 2] = data[i];
            }

            return zrange(dd, false, null);
        }

        if ("zrevrank".equals(cmd)) {
            return zrank(true);
        }

        if ("zscore".equals(cmd)) {
            return zscore();
        }

        if ("zunion".equals(cmd)) {
            return zinter(data, false, null, true);
        }

        if ("zunionstore".equals(cmd)) {
            if (data.length < 5) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length - 1][];
            dd[0] = data[0];
            dd[1] = data[2];
            dd[2] = data[3];
            dd[3] = data[4];

            for (int i = 5; i < data.length; i++) {
                dd[i - 1] = data[i];
            }

            return zinter(dd, true, data[1], true);
        }

        return NilReply.INSTANCE;
    }

    private record Member(double score, String e) {
        @Override
        public String toString() {
            return "Member{" +
                    "score=" + score +
                    ", e='" + e + '\'' +
                    '}';
        }
    }

    private Reply zadd() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var scoreBeginIndex = 2;

        boolean isNx = false;
        boolean isXx = false;
        boolean isGt = false;
        boolean isLt = false;
        boolean isCh = false;
        boolean isIncr = false;

        for (int i = 2; i < data.length; i++) {
            var tmpBytes = data[i];
            var tmp = new String(tmpBytes).toLowerCase();
            if ("nx".equals(tmp)) {
                isNx = true;
                scoreBeginIndex++;
            } else if ("xx".equals(tmp)) {
                isXx = true;
                scoreBeginIndex++;
            } else if ("gt".equals(tmp)) {
                isGt = true;
                scoreBeginIndex++;
            } else if ("lt".equals(tmp)) {
                isLt = true;
                scoreBeginIndex++;
            } else if ("ch".equals(tmp)) {
                isCh = true;
                scoreBeginIndex++;
            } else if ("incr".equals(tmp)) {
                isIncr = true;
                scoreBeginIndex++;
            } else {
                break;
            }
        }

        if (isIncr) {
            // only support one pair
            if (data.length != scoreBeginIndex + 2) {
                return ErrorReply.SYNTAX;
            }
        }

        if (data.length < scoreBeginIndex + 2) {
            return ErrorReply.SYNTAX;
        }

        // multi e
        ArrayList<Member> members = new ArrayList<>();
        for (int i = scoreBeginIndex; i < data.length; i += 2) {
            var scoreBytes = data[i];
            double score;
            try {
                score = Double.parseDouble(new String(scoreBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }

            var memberBytes = data[i + 1];
            if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
                return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
            }

            members.add(new Member(score, new String(memberBytes)));
        }

        // use RedisZSet to store zset
        var slotWithKeyHash = slotPreferParsed(keyBytes);

        var setValueBytes = get(keyBytes, slotWithKeyHash);
        var rz = setValueBytes == null ? new RedisZSet() : RedisZSet.decode(setValueBytes);
        var size = rz.size();

        if (size >= RedisZSet.ZSET_MAX_SIZE) {
            return ErrorReply.ZSET_SIZE_TO_LONG;
        }

        int added = 0;
        int changed = 0;
        for (var member : members) {
            if (isNx) {
                // nx
                if (rz.contains(member.e)) {
                    continue;
                }
            } else if (isXx) {
                // xx
                if (!rz.contains(member.e)) {
                    continue;
                }
            }

            var svExist = rz.get(member.e);
            if (isGt) {
                // gt
                if (svExist != null && svExist.score() <= member.score) {
                    continue;
                }
            } else if (isLt) {
                // lt
                if (svExist != null && svExist.score() >= member.score) {
                    continue;
                }
            }

            var newScore = member.score;
            if (isIncr) {
                // incr
                if (svExist != null) {
                    newScore += svExist.score();
                }
            }

            boolean isNewAdded = rz.add(newScore, member.e);
            if (isNewAdded && (size + 1) == RedisZSet.ZSET_MAX_SIZE) {
                return ErrorReply.ZSET_SIZE_TO_LONG;
            }
            if (isNewAdded) {
                added++;
            } else {
                changed++;
            }
        }

        var encodedBytes = rz.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

        set(keyBytes, encodedBytes, slotWithKeyHash, spType);
        return new IntegerReply(isCh ? changed : added);
    }

    private Reply zcard() {
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

        if (!setCv.isZSet()) {
            return ErrorReply.WRONG_TYPE;
        }

        var setValueBytes = getValueBytesByCv(setCv);
        var size = RedisHashKeys.setSize(setValueBytes);
        return new IntegerReply(size);
    }

    private final static String inf = "+inf";

    private final static String negInf = "-inf";

    private RedisZSet getByKeyBytes(byte[] keyBytes) {
        return getByKeyBytes(keyBytes, null);
    }

    private RedisZSet getByKeyBytes(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        var setCv = getCv(keyBytes, slotWithKeyHashReuse);
        if (setCv == null) {
            return null;
        }
        if (!setCv.isZSet()) {
            // throw exception ?
            return null;
        }

        var setValueBytes = getValueBytesByCv(setCv);
        if (setValueBytes == null) {
            return null;
        }
        return RedisZSet.decode(setValueBytes);
    }

    private Reply zcount(boolean byLex) {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var minBytes = data[2];
        var maxBytes = data[3];
        var minStr = new String(minBytes).toLowerCase();
        var maxStr = new String(maxBytes).toLowerCase();

        String minLex = null;
        String maxLex = null;
        boolean minInclusive = true;
        boolean maxInclusive = true;

        if (byLex) {
            if (minBytes[0] != '(' && minBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }
            if (maxBytes[0] != '(' && maxBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }

            minLex = minStr.substring(1);
            maxLex = maxStr.substring(1);

            if (minLex.compareTo(maxLex) > 0) {
                return IntegerReply.REPLY_0;
            }

            if (minBytes[0] == '(') {
                minInclusive = false;
            }
            if (maxBytes[0] == '(') {
                maxInclusive = false;
            }
        }

        double min = 0;
        double max = 0;
        if (!byLex) {
            if (negInf.equals(minStr)) {
                min = Double.NEGATIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (minBytes[0] == '(') {
                    minInclusive = false;
                    beginIndex = 1;
                } else if (minBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    min = Double.parseDouble(minStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (inf.equals(maxStr)) {
                max = Double.POSITIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                    beginIndex = 1;
                } else if (maxBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    max = Double.parseDouble(maxStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (min > max) {
                return IntegerReply.REPLY_0;
            }
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return IntegerReply.REPLY_0;
        }
        if (rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        if (!byLex) {
            var subSet = rz.between(min, minInclusive, max, maxInclusive);
            // remove min and max if not inclusive
            var itTmp = subSet.iterator();
            while (itTmp.hasNext()) {
                var sv = itTmp.next();
                if (sv.score() == min && !minInclusive) {
                    itTmp.remove();
                }
                if (sv.score() == max && !maxInclusive) {
                    itTmp.remove();
                }
            }

            return new IntegerReply(subSet.size());
        } else {
            int count = rz.betweenByMember(minLex, minInclusive, maxLex, maxInclusive).size();
            return new IntegerReply(count);
        }
    }

    private Reply zdiff() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var numKeysBytes = data[1];
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(numKeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numKeys < 1) {
            return ErrorReply.SYNTAX;
        }

        // withscores
        if (data.length != numKeys + 2 && data.length != numKeys + 3) {
            return ErrorReply.SYNTAX;
        }

        boolean withScores = "withscores".equalsIgnoreCase(new String(data[data.length - 1]));

        var keyBytes = data[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var otherKeyBytesArr = new byte[numKeys - 1][];
        for (int i = 0; i < numKeys - 1; i++) {
            var otherKeyBytes = data[3 + i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i] = otherKeyBytes;
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rz.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRz = getByKeyBytes(otherKeyBytes);
            if (otherRz != null) {
                for (var otherE : otherRz.getSet()) {
                    if (rz.contains(otherE.member())) {
                        rz.remove(otherE.member());
                    }
                }
                if (rz.isEmpty()) {
                    break;
                }
            }
        }

        if (rz.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replyArr = new Reply[rz.size() * (withScores ? 2 : 1)];
        int i = 0;
        for (var sv : rz.getSet()) {
            replyArr[i++] = new BulkReply(sv.member().getBytes());
            if (withScores) {
                replyArr[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
            }
        }
        return new MultiBulkReply(replyArr);
    }

    public Reply zdiffstore() {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        var dstKeyBytes = data[1];
        if (dstKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var numKeysBytes = data[2];
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(numKeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numKeys < 1) {
            return ErrorReply.SYNTAX;
        }

        if (data.length != numKeys + 3) {
            return ErrorReply.SYNTAX;
        }

        var keyBytes = data[3];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var otherKeyBytesArr = new byte[numKeys - 1][];
        for (int i = 0; i < numKeys - 1; i++) {
            var otherKeyBytes = data[4 + i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i] = otherKeyBytes;
        }

        // in slot lock?
        var rz = getByKeyBytes(keyBytes);
        if (rz == null) {
            return IntegerReply.REPLY_0;
        }
        if (rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRz = getByKeyBytes(otherKeyBytes);
            if (otherRz != null) {
                for (var otherE : otherRz.getSet()) {
                    if (rz.contains(otherE.member())) {
                        rz.remove(otherE.member());
                    }
                }
                if (rz.isEmpty()) {
                    break;
                }
            }
        }

        storeRedisZSet(rz, dstKeyBytes);
        return new IntegerReply(rz.size());
    }

    private Reply zincrby() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var scoreBytes = data[2];
        double score;
        try {
            score = Double.parseDouble(new String(scoreBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_FLOAT;
        }

        var memberBytes = data[3];
        if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
            return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);

        var rz = getByKeyBytes(keyBytes, slotWithKeyHash);
        if (rz == null) {
            rz = new RedisZSet();
        }

        var member = new String(memberBytes);
        var svExist = rz.get(member);
        if (svExist != null) {
            score += svExist.score();
        } else {
            if (rz.size() >= RedisZSet.ZSET_MAX_SIZE) {
                return ErrorReply.ZSET_SIZE_TO_LONG;
            }
        }

        rz.add(score, member);

        var encodedBytes = rz.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

        set(keyBytes, encodedBytes, slotWithKeyHash, spType);
        return new BulkReply(String.valueOf(score).getBytes());
    }

    private Reply zinter(byte[][] dd, boolean doStore, byte[] dstKeyBytes, boolean isUnion) {
        if (dd.length < 4) {
            return ErrorReply.FORMAT;
        }

        var numKeysBytes = dd[1];
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(numKeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numKeys < 1) {
            return ErrorReply.SYNTAX;
        }

        var keyBytes = dd[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (dd.length < numKeys + 2) {
            return ErrorReply.SYNTAX;
        }

        var otherKeyBytesArr = new byte[numKeys - 1][];
        for (int i = 0; i < numKeys - 1; i++) {
            var otherKeyBytes = dd[3 + i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i] = otherKeyBytes;
        }

        boolean isAggregateSum = true;
        boolean isAggregateMin = false;
        boolean isAggregateMax = false;

        boolean isWeights = false;
        double[] weights = null;

        boolean withScores = false;

        for (int i = 3 + numKeys - 1; i < dd.length; i++) {
            var tmpBytes = dd[i];
            var tmp = new String(tmpBytes).toLowerCase();

            if ("aggregate".equals(tmp)) {
                if (i + 1 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                var aggregateBytes = dd[i + 1];
                var aggregate = new String(aggregateBytes).toLowerCase();
                if ("sum".equals(aggregate)) {
                    isAggregateSum = true;
                    isAggregateMin = false;
                    isAggregateMax = false;
                } else if ("min".equals(aggregate)) {
                    isAggregateSum = false;
                    isAggregateMin = true;
                    isAggregateMax = false;
                } else if ("max".equals(aggregate)) {
                    isAggregateSum = false;
                    isAggregateMin = false;
                    isAggregateMax = true;
                } else {
                    return ErrorReply.SYNTAX;
                }
                i++;
            } else if ("weights".equals(tmp)) {
                isWeights = true;

                if (i + numKeys >= dd.length) {
                    return ErrorReply.SYNTAX;
                }

                weights = new double[numKeys];
                for (int j = 0; j < numKeys; j++) {
                    var weightBytes = dd[i + 1 + j];
                    double weight;
                    try {
                        weight = Double.parseDouble(new String(weightBytes));
                    } catch (NumberFormatException e) {
                        return ErrorReply.NOT_FLOAT;
                    }
                    weights[j] = weight;
                }
            } else if ("withscores".equals(tmp)) {
                withScores = true;
            }
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rz.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var memberMap = rz.getMemberMap();

        int otherKeyIndex = 0;
        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRz = getByKeyBytes(otherKeyBytes);
            if (otherRz == null) {
                if (!isUnion) {
                    memberMap.clear();
                    break;
                }
            } else {
                if (isUnion) {
                    var otherIt = otherRz.getSet().iterator();
                    while (otherIt.hasNext()) {
                        var otherSv = otherIt.next();
                        var sv = rz.get(otherSv.member());

                        if (sv == null) {
                            double memberScore = otherSv.score();
                            if (isWeights) {
                                memberScore *= weights[otherKeyIndex + 1];
                            }

                            rz.add(memberScore, otherSv.member(), true, true);
                            if (rz.size() == RedisZSet.ZSET_MAX_SIZE) {
                                return ErrorReply.ZSET_SIZE_TO_LONG;
                            }
                        } else {
                            double memberScore = sv.score();
                            if (!sv.isAlreadyWeighted) {
                                if (isWeights) {
                                    memberScore *= weights[0];
                                }
                                sv.score(memberScore);
                                sv.isAlreadyWeighted = true;
                            }

                            var otherMemberScore = isWeights ? weights[otherKeyIndex + 1] * otherSv.score() : otherSv.score();
                            if (isAggregateSum) {
                                sv.score(memberScore + otherMemberScore);
                            } else if (isAggregateMin) {
                                sv.score(Math.min(memberScore, otherMemberScore));
                            } else if (isAggregateMax) {
                                sv.score(Math.max(memberScore, otherMemberScore));
                            }
                        }
                    }
                } else {
                    var it = rz.getSet().iterator();
                    while (it.hasNext()) {
                        var sv = it.next();
                        var otherSv = otherRz.get(sv.member());
                        if (otherSv == null) {
                            it.remove();
                            memberMap.remove(sv.member());
                            continue;
                        }

                        double memberScore = sv.score();
                        if (!sv.isAlreadyWeighted) {
                            if (isWeights) {
                                memberScore *= weights[0];
                            }
                            sv.score(memberScore);
                            sv.isAlreadyWeighted = true;
                        }

                        var otherMemberScore = isWeights ? weights[otherKeyIndex + 1] * otherSv.score() : otherSv.score();
                        if (isAggregateSum) {
                            sv.score(memberScore + otherMemberScore);
                        } else if (isAggregateMin) {
                            sv.score(Math.min(memberScore, otherMemberScore));
                        } else if (isAggregateMax) {
                            sv.score(Math.max(memberScore, otherMemberScore));
                        }
                    }
                    if (memberMap.isEmpty()) {
                        break;
                    }
                }
            }
            otherKeyIndex++;
        }

        if (doStore) {
            var dstRz = new RedisZSet();
            for (var sv : memberMap.entrySet()) {
                // resort
                dstRz.add(sv.getValue().score(), sv.getKey());
            }
            storeRedisZSet(dstRz, dstKeyBytes);
            return new IntegerReply(memberMap.size());
        }

        if (memberMap.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[memberMap.size() * (withScores ? 2 : 1)];
        TreeSet<RedisZSet.ScoreValue> sortedSet = new TreeSet<>();
        for (var e : memberMap.entrySet()) {
            sortedSet.add(new RedisZSet.ScoreValue(e.getValue().score(), e.getKey()));
        }

        int i = 0;
        for (var sv : sortedSet) {
            replies[i++] = new BulkReply(sv.member().getBytes());
            if (withScores) {
                replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
            }
        }
        return new MultiBulkReply(replies);
    }

    private Reply zintercard() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var numKeysBytes = data[1];
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(numKeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numKeys < 1) {
            return ErrorReply.SYNTAX;
        }

        var keyBytes = data[2];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        if (data.length < numKeys + 2) {
            return ErrorReply.SYNTAX;
        }

        var otherKeyBytesArr = new byte[numKeys - 1][];
        for (int i = 0; i < numKeys - 1; i++) {
            var otherKeyBytes = data[3 + i];
            if (otherKeyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            otherKeyBytesArr[i] = otherKeyBytes;
        }

        int limit = 0;
        for (int i = 3 + numKeys - 1; i < data.length; i++) {
            var tmpBytes = data[i];
            var tmp = new String(tmpBytes).toLowerCase();

            if ("limit".equals(tmp)) {
                if (i + 1 >= data.length) {
                    return ErrorReply.SYNTAX;
                }
                var limitBytes = data[i + 1];
                try {
                    limit = Integer.parseInt(new String(limitBytes));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                i++;
            }
        }
        if (limit < 0) {
            return ErrorReply.INVALID_INTEGER;
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return IntegerReply.REPLY_0;
        }
        if (rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        var set = rz.getSet();
//        var memberMap = rz.getMemberMap();

        for (var otherKeyBytes : otherKeyBytesArr) {
            var otherRz = getByKeyBytes(otherKeyBytes);
            if (otherRz != null) {
                var it = set.iterator();
                while (it.hasNext()) {
                    var sv = it.next();
                    if (!otherRz.contains(sv.member())) {
                        it.remove();
                        // not necessary
//                        memberMap.remove(sv.member());
                    }
                }
                if (set.isEmpty()) {
                    break;
                }
                if (limit != 0 && set.size() >= limit) {
                    break;
                }
            }
        }

        var n = limit == 0 ? set.size() : Math.min(set.size(), limit);
        return new IntegerReply(n);
    }

    private Reply zmscore() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 0; i < data.length - 2; i++) {
            var memberBytes = data[2 + i];
            if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
                return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i] = memberBytes;
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[memberBytesArr.length];
        int i = 0;
        for (var memberBytes : memberBytesArr) {
            var sv = rz.get(new String(memberBytes));
            if (sv == null) {
                replies[i++] = NilReply.INSTANCE;
            } else {
                replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
            }
        }
        return new MultiBulkReply(replies);
    }

    private Reply zpopmax(boolean isMin) {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int count = 1;
        if (data.length == 3) {
            var countBytes = data[2];
            try {
                count = Integer.parseInt(new String(countBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (count <= 0) {
                return ErrorReply.INVALID_INTEGER;
            }
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var slot = slotWithKeyHash.slot();

        var rz = getByKeyBytes(keyBytes, slotWithKeyHash);
        if (rz == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rz.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[Math.min(count, rz.size()) * 2];
        for (int j = 0; j < replies.length; j += 2) {
            var sv = isMin ? rz.pollFirst() : rz.pollLast();
            replies[j] = new BulkReply(sv.member().getBytes());
            replies[j + 1] = new BulkReply(String.valueOf(sv.score()).getBytes());
        }

        if (rz.isEmpty()) {
            var oneSlot = localPersist.oneSlot(slot);
            // remove key
            oneSlot.removeDelay(new String(keyBytes), slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
        } else {
            var encodedBytes = rz.encode();
            var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
            var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

            set(keyBytes, encodedBytes, slotWithKeyHash, spType);
        }
        return new MultiBulkReply(replies);
    }

    private Reply zrandmember() {
        if (data.length < 2 || data.length > 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int count = 1;
        boolean hasCount = false;
        for (int i = 2; i < data.length; i++) {
            var tmpBytes = data[i];
            var tmp = new String(tmpBytes).toLowerCase();
            if (!"withscores".equals(tmp)) {
                try {
                    count = Integer.parseInt(tmp);
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                if (count <= 0) {
                    return ErrorReply.INVALID_INTEGER;
                }
                hasCount = true;
                i++;
            } else {
                if (data.length > i + 1) {
                    return ErrorReply.SYNTAX;
                }
                break;
            }
        }

        boolean withScores = "withscores".equalsIgnoreCase(new String(data[data.length - 1]));

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }
        if (rz.isEmpty()) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = rz.size();
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

        var replies = new Reply[absCount * (withScores ? 2 : 1)];

        int j = 0;
        var it = rz.getSet().iterator();
        while (it.hasNext()) {
            var sv = it.next();

            for (int k = 0; k < indexes.size(); k++) {
                Integer index = indexes.get(k);
                if (index != null && index == j) {
                    replies[k * (withScores ? 2 : 1)] = new BulkReply(sv.member().getBytes());
                    if (withScores) {
                        replies[k * 2 + 1] = new BulkReply(String.valueOf(sv.score()).getBytes());
                    }
                    indexes.set(k, null);
                    if (indexes.isEmpty()) {
                        break;
                    }
                }
            }
            j++;
        }

        return new MultiBulkReply(replies);
    }

    private void storeRedisZSet(RedisZSet rz, byte[] dstKeyBytes) {
        var slotWithKeyHash = slot(dstKeyBytes);
        var slot = slotWithKeyHash.slot();
//        var bucketIndex = keyLoader.bucketIndex(slotWithKeyHash.keyHash());
//        keyLoader.bucketLock(slot, bucketIndex, () -> {
        if (rz.isEmpty()) {
            var oneSlot = localPersist.oneSlot(slot);
            // remove dst key
            oneSlot.removeDelay(new String(dstKeyBytes), slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
            return;
        }

        var encodedBytes = rz.encode();
        var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
        var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

        set(dstKeyBytes, encodedBytes, slotWithKeyHash, spType);
//        });
    }

    private Reply zrange(byte[][] dd, boolean doStore, byte[] dstKeyBytes) {
        if (dd.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = dd[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var minBytes = dd[2];
        var maxBytes = dd[3];
        var minStr = new String(minBytes).toLowerCase();
        var maxStr = new String(maxBytes).toLowerCase();

        boolean byIndex = true;
        boolean byScore = false;
        boolean byLex = false;
        boolean isReverse = false;
        boolean hasLimit = false;
        int offset = 0;
        int count = 0;
        boolean withScores = false;

        for (int i = 4; i < dd.length; i++) {
            var tmpBytes = dd[i];
            var tmp = new String(tmpBytes).toLowerCase();
            if ("byscore".equals(tmp)) {
                byIndex = false;
                byScore = true;
                byLex = false;
            } else if ("bylex".equals(tmp)) {
                byIndex = false;
                byScore = false;
                byLex = true;
            } else if ("rev".equals(tmp)) {
                isReverse = true;
            } else if ("limit".equals(tmp)) {
                hasLimit = true;

                if (i + 2 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                var offsetBytes = dd[i + 1];
                try {
                    offset = Integer.parseInt(new String(offsetBytes));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                if (offset < 0) {
                    return ErrorReply.INVALID_INTEGER;
                }

                var countBytes = dd[i + 2];
                try {
                    count = Integer.parseInt(new String(countBytes));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                i += 2;
            } else if ("withscores".equals(tmp)) {
                withScores = true;
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        String minLex = null;
        String maxLex = null;
        boolean minInclusive = true;
        boolean maxInclusive = true;

        if (byLex) {
            if (minBytes[0] != '(' && minBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }
            if (maxBytes[0] != '(' && maxBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }

            minLex = minStr.substring(1);
            maxLex = maxStr.substring(1);

            int compareMinMax = minLex.compareTo(maxLex);
            if (compareMinMax > 0 && !isReverse) {
                return IntegerReply.REPLY_0;
            }
            if (compareMinMax < 0 && isReverse) {
                return IntegerReply.REPLY_0;
            }

            if (minBytes[0] == '(') {
                minInclusive = false;
            }
            if (maxBytes[0] == '(') {
                maxInclusive = false;
            }

//            if (isReverse) {
//                String tmp = minLex;
//                minLex = maxLex;
//                maxLex = tmp;
//            }
        }

        double min = 0;
        double max = 0;
        if (byScore) {
            if (negInf.equals(minStr)) {
                min = Double.NEGATIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (minBytes[0] == '(') {
                    minInclusive = false;
                    beginIndex = 1;
                } else if (minBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    min = Double.parseDouble(minStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (inf.equals(maxStr)) {
                max = Double.POSITIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                    beginIndex = 1;
                } else if (maxBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    max = Double.parseDouble(maxStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (isReverse) {
                if (min > max) {
                    double tmp = min;
                    min = max;
                    max = tmp;

                    boolean tmp2 = minInclusive;
                    minInclusive = maxInclusive;
                    maxInclusive = tmp2;
                } else {
                    return IntegerReply.REPLY_0;
                }
            } else {
                if (min > max) {
                    return IntegerReply.REPLY_0;
                }
            }
        }

        int start = 0;
        int stop = -1;
        if (byIndex) {
            try {
                start = Integer.parseInt(minStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            try {
                stop = Integer.parseInt(maxStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return MultiBulkReply.EMPTY;
        }
        if (rz.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        if (count <= 0) {
            count = rz.size();
        }

        if (byIndex) {
            int size = rz.size();
            if (start < 0) {
                start = size + start;
                if (start < 0) {
                    start = 0;
                }
            }
            if (stop < 0) {
                stop = size + stop;
                if (stop < 0) {
                    return MultiBulkReply.EMPTY;
                }
            }
            if (start >= size) {
                return MultiBulkReply.EMPTY;
            }
            if (stop >= size) {
                stop = size - 1;
            }
            if (start > stop) {
                return MultiBulkReply.EMPTY;
            }

            var dstRz = new RedisZSet();
            var replies = hasLimit ? new Reply[Math.min(stop - start + 1, count) * (withScores ? 2 : 1)] :
                    new Reply[(stop - start + 1) * (withScores ? 2 : 1)];

            var it = isReverse ? rz.getSet().descendingSet().iterator() : rz.getSet().iterator();
            int i = 0;
            int j = 0;
            int skip = offset;
            while (it.hasNext()) {
                var sv = it.next();
                if (j >= start && j <= stop) {
                    if (hasLimit) {
                        if (skip > 0) {
                            skip--;
                            j++;
                            continue;
                        }
                    }

                    if (doStore) {
                        dstRz.add(sv.score(), sv.member());
                    } else {
                        replies[i++] = new BulkReply(sv.member().getBytes());
                        if (withScores) {
                            replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
                        }
                        // exceed count
                        if (i >= replies.length) {
                            break;
                        }
                    }
                }
                j++;
            }

            if (doStore) {
                storeRedisZSet(dstRz, dstKeyBytes);
                return new IntegerReply(dstRz.size());
            }

            // offset is too big, replies include null
            if (i == 0) {
                return MultiBulkReply.EMPTY;
            } else if (i < replies.length) {
                replies = Arrays.copyOf(replies, i);
            }

            return new MultiBulkReply(replies);
        } else if (byLex) {
            var subMap = rz.betweenByMember(minLex, minInclusive, maxLex, maxInclusive);
            if (subMap.isEmpty()) {
                return MultiBulkReply.EMPTY;
            }

            var it = isReverse ? subMap.descendingMap().entrySet().iterator() : subMap.entrySet().iterator();
            if (hasLimit) {
                int skip = offset;
                while (skip > 0 && it.hasNext()) {
                    it.next();
                    it.remove();
                    skip--;
                }
            }

            if (count <= 0) {
                count = subMap.size();
            }

            if (doStore) {
                var dstRz = new RedisZSet();
                int storedCount = 0;
                var it2 = subMap.entrySet().iterator();
                while (it2.hasNext()) {
                    if (storedCount >= count) {
                        break;
                    }
                    var entry = it2.next();
                    dstRz.add(entry.getValue().score(), entry.getKey());
                    storedCount++;
                }
                storeRedisZSet(dstRz, dstKeyBytes);
                return new IntegerReply(dstRz.size());
            }

            var replies = hasLimit ? new Reply[Math.min(subMap.size(), count) * (withScores ? 2 : 1)] :
                    new Reply[subMap.size() * (withScores ? 2 : 1)];
            var it2 = subMap.entrySet().iterator();
            int i = 0;
            while (it2.hasNext()) {
                var entry = it2.next();
                replies[i++] = new BulkReply(entry.getKey().getBytes());
                if (withScores) {
                    replies[i++] = new BulkReply(String.valueOf(entry.getValue().score()).getBytes());
                }
                // exceed count
                if (i >= replies.length) {
                    break;
                }
            }
            return new MultiBulkReply(replies);
        } else {
            var subSet = rz.between(min, minInclusive, max, maxInclusive);

            // remove min and max if not inclusive
            var itTmp = subSet.iterator();
            while (itTmp.hasNext()) {
                var sv = itTmp.next();
                if (sv.score() == min && !minInclusive) {
                    itTmp.remove();
                }
                if (sv.score() == max && !maxInclusive) {
                    itTmp.remove();
                }
            }

            if (subSet.isEmpty()) {
                return MultiBulkReply.EMPTY;
            }

            var it = isReverse ? subSet.descendingSet().iterator() : subSet.iterator();
            if (hasLimit) {
                int skip = offset;
                while (skip > 0 && it.hasNext()) {
                    it.next();
                    it.remove();
                    skip--;
                }
            }

            if (count <= 0) {
                count = subSet.size();
            }

            if (doStore) {
                var dstRz = new RedisZSet();
                int storedCount = 0;
                var it2 = subSet.iterator();
                while (it2.hasNext()) {
                    if (storedCount >= count) {
                        break;
                    }
                    var entry = it2.next();
                    dstRz.add(entry.score(), entry.member());
                    storedCount++;
                }
                storeRedisZSet(dstRz, dstKeyBytes);
                return new IntegerReply(dstRz.size());
            }

            var replies = hasLimit ? new Reply[Math.min(subSet.size(), count) * (withScores ? 2 : 1)] :
                    new Reply[subSet.size() * (withScores ? 2 : 1)];
            var it2 = isReverse ? subSet.descendingSet().iterator() : subSet.iterator();
            int i = 0;
            while (it2.hasNext()) {
                var entry = it2.next();
                replies[i++] = new BulkReply(entry.member().getBytes());
                if (withScores) {
                    replies[i++] = new BulkReply(String.valueOf(entry.score()).getBytes());
                }
                // exceed count
                if (i >= replies.length) {
                    break;
                }
            }
            return new MultiBulkReply(replies);
        }
    }

    private Reply zrank(boolean isReverse) {
        if (data.length != 3 && data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[2];
        if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
            return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
        }

        boolean withScores = "withscores".equalsIgnoreCase(new String(data[data.length - 1]));

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return NilReply.INSTANCE;
        }
        if (rz.isEmpty()) {
            return NilReply.INSTANCE;
        }

        var sv = rz.get(new String(memberBytes));
        if (sv == null) {
            return NilReply.INSTANCE;
        }

        int rank = sv.getInitRank();
        if (isReverse) {
            rank = rz.size() - rank - 1;
        }

        if (!withScores) {
            return new IntegerReply(rank);
        }

        var replies = new Reply[2];
        replies[0] = new IntegerReply(rank);
        replies[1] = new BulkReply(String.valueOf(sv.score()).getBytes());
        return new MultiBulkReply(replies);
    }

    private Reply zrem() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 0; i < data.length - 2; i++) {
            var memberBytes = data[2 + i];
            if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
                return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i] = memberBytes;
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var slot = slotWithKeyHash.slot();

        var rz = getByKeyBytes(keyBytes, slotWithKeyHash);
        if (rz == null) {
            return IntegerReply.REPLY_0;
        }
        if (rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        int removedCount = 0;
        for (var memberBytes : memberBytesArr) {
            var isRemoved = rz.remove(new String(memberBytes));
            if (isRemoved) {
                removedCount++;
            }
        }

        if (rz.isEmpty()) {
            // remove key
            var oneSlot = localPersist.oneSlot(slot);
            oneSlot.removeDelay(new String(keyBytes), slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
        } else {
            var encodedBytes = rz.encode();
            var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
            var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

            set(keyBytes, encodedBytes, slotWithKeyHash, spType);
        }
        return new IntegerReply(removedCount);
    }

    private Reply zremrangebyscore(boolean byScore, boolean byLex, boolean byRank) {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var minBytes = data[2];
        var maxBytes = data[3];
        var minStr = new String(minBytes).toLowerCase();
        var maxStr = new String(maxBytes).toLowerCase();

        String minLex;
        String maxLex;
        boolean minInclusive = true;
        boolean maxInclusive = true;

        if (byLex) {
            if (minBytes[0] != '(' && minBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }
            if (maxBytes[0] != '(' && maxBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }

            minLex = minStr.substring(1);
            maxLex = maxStr.substring(1);

            int compareMinMax = minLex.compareTo(maxLex);
            if (compareMinMax > 0) {
                return IntegerReply.REPLY_0;
            }
            if (compareMinMax < 0) {
                return IntegerReply.REPLY_0;
            }

            if (minBytes[0] == '(') {
                minInclusive = false;
            }
            if (maxBytes[0] == '(') {
                maxInclusive = false;
            }
        } else {
            minLex = null;
            maxLex = null;
        }

        double min = 0;
        double max = 0;
        if (byScore) {
            if (negInf.equals(minStr)) {
                min = Double.NEGATIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (minBytes[0] == '(') {
                    minInclusive = false;
                    beginIndex = 1;
                } else if (minBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    min = Double.parseDouble(minStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (inf.equals(maxStr)) {
                max = Double.POSITIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                    beginIndex = 1;
                } else if (maxBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    max = Double.parseDouble(maxStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (min > max) {
                return IntegerReply.REPLY_0;
            }
        }

        int start = 0;
        int stop = -1;
        if (byRank) {
            try {
                start = Integer.parseInt(minStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            try {
                stop = Integer.parseInt(maxStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);
        var slot = slotWithKeyHash.slot();

        var rz = getByKeyBytes(keyBytes, slotWithKeyHash);
        if (rz == null) {
            return IntegerReply.REPLY_0;
        }
        if (rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        int removed = 0;
        var it = rz.getSet().iterator();
        while (it.hasNext()) {
            var sv = it.next();
            if (byLex) {
                int compMin = sv.member().compareTo(minLex);
                int compMax = sv.member().compareTo(maxLex);
                if (compMin < 0 || (compMin == 0 && !minInclusive)) {
                    continue;
                }
                if (compMax > 0 || (compMax == 0 && !maxInclusive)) {
                    continue;
                }

                it.remove();
                removed++;
            } else if (byScore) {
                double score = sv.score();
                if (score < min || (score == min && !minInclusive)) {
                    continue;
                }
                if (score > max || (score == max && !maxInclusive)) {
                    continue;
                }

                it.remove();
                removed++;
            } else if (byRank) {
                int initRank = sv.getInitRank();

                if (start < 0) {
                    start = rz.size() + start;
                    if (start < 0) {
                        start = 0;
                    }
                }

                if (stop < 0) {
                    stop = rz.size() + stop;
                    if (stop < 0) {
                        return IntegerReply.REPLY_0;
                    }
                }

                if (initRank < start || initRank > stop) {
                    continue;
                }

                it.remove();
                removed++;
            }
        }

        if (removed > 0) {
            if (rz.isEmpty()) {
                // remove key
                var oneSlot = localPersist.oneSlot(slot);
                oneSlot.removeDelay(new String(keyBytes), slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash());
            } else {
                var encodedBytes = rz.encode();
                var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

                set(keyBytes, encodedBytes, slotWithKeyHash, spType);
            }
        }

        return new IntegerReply(removed);
    }

    private Reply zscore() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[2];
        if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
            return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
        }

        var rz = getByKeyBytes(keyBytes, slotPreferParsed(keyBytes));
        if (rz == null) {
            return NilReply.INSTANCE;
        }

        var sv = rz.get(new String(memberBytes));
        if (sv == null) {
            return NilReply.INSTANCE;
        }

        return new BulkReply(String.valueOf(sv.score()).getBytes());
    }
}
