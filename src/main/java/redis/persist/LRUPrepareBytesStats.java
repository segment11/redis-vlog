package redis.persist;

import java.util.ArrayList;

public class LRUPrepareBytesStats {
    private LRUPrepareBytesStats() {
    }

    enum Type {
        fd_key_bucket, fd_chunk_data, big_string, kv_read_group_by_wal_group, kv_write_in_wal, chunk_segment_merged_cv_buffer
    }

    record One(Type type, int lruMemoryRequireMB, boolean isExact) {
    }

    static ArrayList<One> list = new ArrayList<>();

    static void add(Type type, int lruMemoryRequireMB, boolean isExact) {
        list.add(new One(type, lruMemoryRequireMB, isExact));
    }

    static int sum() {
        return list.stream().mapToInt(one -> one.lruMemoryRequireMB).sum();
    }

    static int sum(Type type) {
        return list.stream().filter(one -> one.type == type).mapToInt(one -> one.lruMemoryRequireMB).sum();
    }
}
