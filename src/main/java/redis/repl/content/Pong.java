package redis.repl.content;

import redis.repl.ReplContent;

public class Pong extends Ping implements ReplContent {
    public Pong(String netListenAddresses) {
        super(netListenAddresses);
    }
}
