package redis.repl.content;

import redis.repl.ReplContent;

public class StringContent implements ReplContent {
    private final String message;

    public StringContent(byte[] bytes) {
        this.message = new String(bytes);
    }

    @Override
    public byte[] encode() {
        return message.getBytes();
    }
}
