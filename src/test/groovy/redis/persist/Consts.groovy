package redis.persist

class Consts {
    static final File slotDir = new File('/tmp/redis-vlog/test-persist/test-slot')
    static final File persistDir = new File('/tmp/redis-vlog/test-persist/')

    static {
        slotDir.mkdirs()
    }
}
