package redis.type;

import org.jetbrains.annotations.NotNull;
import redis.KeyHash;

import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class RedisZSet {
    // change here to limit zset size
    // values encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose value length is 32, then 16KB / 32 = 512
    public static final short ZSET_MAX_SIZE = 4096;

    public static final int ZSET_MEMBER_MAX_LENGTH = 255;

    // set size short + crc int
    private static final int HEADER_LENGTH = 2 + 4;

    public static class ScoreValue implements Comparable<ScoreValue> {
        private double score;
        private final String member;

        public ScoreValue(double score, @NotNull String member) {
            this.score = score;
            this.member = member;
        }

        private int initRank = 0;

        public int getInitRank() {
            return initRank;
        }

        public void setInitRank(int initRank) {
            this.initRank = initRank;
        }

        public boolean isAlreadyWeighted = false;

        public double score() {
            return score;
        }

        public void score(double score) {
            this.score = score;
        }

        public String member() {
            return member;
        }

        @Override
        public int compareTo(@NotNull RedisZSet.ScoreValue o) {
            if (o == this) {
                return 0;
            }

            int compScore = Double.compare(score, o.score);
            if (compScore != 0) {
                return compScore;
            }

            if (member.equals(MAX_MEMBER)) {
                return 1;
            }
            if (o.member.equals(MAX_MEMBER)) {
                return -1;
            }
            return member.compareTo(o.member);
        }

        @Override
        public String toString() {
            return "ScoreValue{" +
                    "score=" + score +
                    ", member='" + member + '\'' +
                    '}';
        }

        public int length() {
            // score double + value length short
            return 8 + member.length();
        }
    }

    private final ConcurrentSkipListSet<ScoreValue> set = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListMap<String, ScoreValue> memberMap = new ConcurrentSkipListMap<>();

    // need not thread safe
    public ConcurrentSkipListSet<ScoreValue> getSet() {
        return set;
    }

    public ConcurrentSkipListMap<String, ScoreValue> getMemberMap() {
        return memberMap;
    }

    static final String MAX_MEMBER = "!z...";

    public NavigableSet<ScoreValue> between(double min, boolean minInclusive, double max, boolean maxInclusive) {
        return set.subSet(new ScoreValue(min, ""), minInclusive,
                new ScoreValue(max, MAX_MEMBER), maxInclusive);
    }

    public ConcurrentNavigableMap<String, ScoreValue> betweenByMember(String min, boolean minInclusive, String max, boolean maxInclusive) {
        return memberMap.subMap(min, minInclusive, max, maxInclusive);
    }

    public int size() {
        return memberMap.size();
    }

    public boolean isEmpty() {
        return memberMap.isEmpty();
    }

    public boolean contains(String member) {
        return memberMap.containsKey(member);
    }

    public boolean remove(String member) {
        var sv = memberMap.get(member);
        if (sv == null) {
            return false;
        }
        memberMap.remove(member);
        return set.remove(sv);
    }

    public ScoreValue pollFirst() {
        var sv = set.pollFirst();
        if (sv == null) {
            return null;
        }
        memberMap.remove(sv.member);
        return sv;
    }

    public ScoreValue pollLast() {
        var sv = set.pollLast();
        if (sv == null) {
            return null;
        }
        memberMap.remove(sv.member);
        return sv;
    }

    public boolean add(double score, String member) {
        return add(score, member, true, false);
    }

    public boolean add(double score, String member, boolean overwrite, boolean isAlreadyWeighted) {
        var svExist = memberMap.get(member);
        if (svExist != null) {
            if (!overwrite) {
                return false;
            }

            memberMap.remove(member);
            set.remove(svExist);

            var sv = new ScoreValue(score, member);
            sv.isAlreadyWeighted = isAlreadyWeighted;
            set.add(sv);
            memberMap.put(member, sv);
            return true;
        } else {
            var sv = new ScoreValue(score, member);
            sv.isAlreadyWeighted = isAlreadyWeighted;
            set.add(sv);
            memberMap.put(member, sv);
            return true;
        }
    }

    public ScoreValue get(String member) {
        return memberMap.get(member);
    }

    public void print() {
        for (var member : set) {
            System.out.println(member);
        }
    }

    public byte[] encode() {
        int len = 0;
        for (var member : set) {
            // zset value length use 2 bytes
            len += 2 + member.length();
        }

        var buffer = ByteBuffer.allocate(len + HEADER_LENGTH);
        buffer.putShort((short) set.size());
        // tmp crc
        buffer.putInt(0);
        for (var e : set) {
            buffer.putShort((short) e.length());
            buffer.putDouble(e.score);
            buffer.put(e.member.getBytes());
        }

        // crc
        if (len > 0) {
            var hb = buffer.array();
            int crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(2, crc);
        }

        return buffer.array();
    }

    public static RedisZSet decode(byte[] data) {
        return decode(data, true);
    }

    public static RedisZSet decode(byte[] data, boolean doCheckCrc32) {
        var buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();
        int crc = buffer.getInt();

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        var r = new RedisZSet();
        int rank = 0;
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            double score = buffer.getDouble();
            var bytes = new byte[len - 8];
            buffer.get(bytes);
            var member = new String(bytes);
            var sv = new ScoreValue(score, member);

            sv.setInitRank(rank);
            rank++;

            r.set.add(sv);
            r.memberMap.put(member, sv);
        }
        return r;
    }
}
