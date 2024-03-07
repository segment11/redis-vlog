package redis.stats;

public record StatKV(String key, double value) {
    public static StatKV split = new StatKV("------", 0);
}
