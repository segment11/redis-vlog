package redis.persist

class Consts {
    static final File slotDir = new File('/tmp/redis-vlog/test-slot')

    static {
        slotDir.mkdirs()
    }
}
