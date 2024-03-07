package redis;

public class Debug {
    // singleton
    private static final Debug instance = new Debug();

    private Debug() {
    }

    public static Debug getInstance() {
        return instance;
    }

    public boolean logMerge = false;

    public boolean logCompress = false;

    public boolean logPersist = false;

    public boolean logRestore = false;

    public boolean perfSkipPersist = false;

    public boolean perfSkipPvmUpdate = false;

    public boolean perfTestReadSegmentNoCache = false;
}
