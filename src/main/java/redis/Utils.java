package redis;

public class Utils {
    private Utils() {
    }

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
}
