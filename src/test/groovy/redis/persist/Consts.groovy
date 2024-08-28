package redis.persist

import org.apache.commons.net.telnet.TelnetClient

class Consts {
    static final File slotDir = new File('/tmp/redis-vlog/test-persist/test-slot')
    static final File slotDir2 = new File('/tmp/redis-vlog/test-persist/test-slot2')
    static final File persistDir = new File('/tmp/redis-vlog/test-persist/')
    static final File testDir = new File('/tmp/test-redis-vlog/')

    static {
        slotDir.mkdirs()
    }

    static boolean checkConnectAvailable(String host = 'localhost', int port = 2181) {
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect(host, port)
            return true
        } catch (Exception ignored) {
            return false
        } finally {
            tc.disconnect()
        }
    }
}
