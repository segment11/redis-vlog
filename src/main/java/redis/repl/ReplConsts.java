package redis.repl;

public class ReplConsts {
    // redis clients can connect to readonly slave instances use this suffix as sentinel master name
    public static final String REPL_MASTER_NAME_READONLY_SLAVE_SUFFIX = "/readonly_slave";
}
