package redis.repl;

public enum ReplType {
    chunk_segment(false, (byte) 0),
    chunk_merge_flag_mmap(false, (byte) 1),
    chunk_segment_index_mmap(false, (byte) 2),
    top_chunk_segment_index_mmap(false, (byte) 3),

    big_string(false, (byte) 4),
    key_bucket(false, (byte) 8),
    wal(true, (byte) 16),
    dict(false, (byte) 32);

    private final boolean newly;
    private final byte type;

    ReplType(boolean newly, byte type) {
        this.newly = newly;
        this.type = type;
    }
}
