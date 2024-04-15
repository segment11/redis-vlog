package redis;

import redis.stats.StatKV;

import java.util.List;

public class Utils {
    public static String rightPad(String s, String pad, int len) {
        if (s.length() >= len) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < len) {
            sb.append(pad);
        }
        return sb.toString();
    }

    public static String leftPad(String s, String pad, int len) {
        if (s.length() >= len) {
            return s;
        }
        var sb = new StringBuilder();
        while (sb.length() < len - s.length()) {
            sb.append(pad);
        }
        sb.append(s);
        return sb.toString();
    }

    public static String padStats(List<StatKV> list, int padLen) {
        var sb = new StringBuilder();
        for (var kv : list) {
            if (kv == StatKV.split) {
                sb.append(kv.key()).append("\n");
                continue;
            }

            sb.append(rightPad(kv.key(), " ", padLen)).append(" ").append(kv.value()).append("\n");
        }
        return sb.toString();
    }
}
