package redis.persist;

import java.util.ArrayList;

public class LRUPrepareBytesStats {
    enum Type {
        fd_key_bucket, fd_chunk_data, big_string, kv_by_wal_group
    }

    record One(Type type, int lruMemoryRequireMB, boolean isExact) {
    }

    static ArrayList<One> list = new ArrayList<>();

    static void add(Type type, int lruMemoryRequireMB, boolean isExact) {
        list.add(new One(type, lruMemoryRequireMB, isExact));
    }

    static int sum(Type type) {
        return list.stream().filter(one -> one.type == type).mapToInt(one -> one.lruMemoryRequireMB).sum();
    }
}
