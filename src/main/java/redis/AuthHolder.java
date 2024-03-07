package redis;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class AuthHolder {
    // need not thread-safe
    static Map<InetSocketAddress, Boolean> flagBySocketAddress = new HashMap<>();
}
