package redis;

public class ConfForGlobal {
    private ConfForGlobal() {
    }

    public static long estimateKeyNumber;
    private static final int DEFAULT_ESTIMATE_ONE_VALUE_LENGTH = 200;
    public static int estimateOneValueLength = DEFAULT_ESTIMATE_ONE_VALUE_LENGTH;
    static final int MAX_ESTIMATE_ONE_VALUE_LENGTH = 4000;

    // compression
    public static boolean isValueSetUseCompression = true;
    public static boolean isOnDynTrainDictForCompression = true;

    public static String netListenAddresses;

    public static String dirPath = "/tmp/redis-vlog";
    public static boolean pureMemory = false;
    public static short slotNumber = 1;
    public static byte netWorkers = 1;
    public static int eventLoopIdleMillis = 10;

    // for repl leader select
    public static String zookeeperConnectString;
    public static String zookeeperRootPath;
    public static boolean canBeLeader = true;
    // for cascade replication
    public static boolean isAsSlaveOfSlave = false;

    public static final String LEADER_LATCH_PATH = "/leader_latch";
    public static final String LEADER_LISTEN_ADDRESS_PATH = "/leader_listen_address";
}
