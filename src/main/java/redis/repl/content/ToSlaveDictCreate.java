package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.Dict;
import redis.repl.ReplContent;

public class ToSlaveDictCreate implements ReplContent {
    private final String key;
    private final Dict dict;

    public ToSlaveDictCreate(String key, Dict dict) {
        this.key = key;
        this.dict = dict;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        var bytes = dict.encode(key);
        toBuf.write(bytes);
    }

    @Override
    public int encodeLength() {
        return dict.encodeLength(key);
    }
}
