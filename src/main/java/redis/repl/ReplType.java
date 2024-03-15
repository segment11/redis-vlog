package redis.repl;

public enum ReplType {
    error(true, (byte) 1, (byte) -1),
    ping(true, (byte) 0, (byte) 0),
    pong(true, (byte) 1, (byte) 1),
    hello(true, (byte) 0, (byte) 2),
    hi(true, (byte) 1, (byte) 3),
    ok(true, (byte) 1, (byte) 4),
    bye(true, (byte) 0, (byte) 5),

    chunk_segment(false, (byte) 1, (byte) 10),
    chunk_merge_flag_mmap(false, (byte) 1, (byte) 11),
    chunk_segment_index_mmap(false, (byte) 1, (byte) 12),
    top_chunk_segment_index_mmap(false, (byte) 1, (byte) 13),
    big_string(false, (byte) 1, (byte) 14),

    meta_chunk_segment(false, (byte) 0, (byte) 20),
    meta_chunk_merge_flag_mmap(false, (byte) 0, (byte) 21),
    meta_chunk_segment_index_mmap(false, (byte) 0, (byte) 22),
    meta_top_chunk_segment_index_mmap(false, (byte) 0, (byte) 23),
    meta_big_string(false, (byte) 0, (byte) 24),

    key_bucket(false, (byte) 1, (byte) 30),
    wal(true, (byte) 1, (byte) 31),
    dict(false, (byte) 1, (byte) 32),

    meta_key_bucket(false, (byte) 0, (byte) 40),
    meta_wal(false, (byte) 0, (byte) 41),
    meta_dict(false, (byte) 0, (byte) 42),
    ;

    public final boolean newly;
    public final byte clientOrServerSide;
    public final byte code;

    public static final byte CLIENT_SIDE = 0;
    public static final byte SERVER_SIDE = 1;

    ReplType(boolean newly, byte clientOrServerSide, byte code) {
        this.newly = newly;
        this.clientOrServerSide = clientOrServerSide;
        this.code = code;
    }

    public boolean isServerSide() {
        return clientOrServerSide == SERVER_SIDE;
    }

    public static ReplType fromCode(byte code) {
        for (var value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }
}
