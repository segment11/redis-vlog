package redis.stats;

import java.util.List;

public interface OfStats {
    List<StatKV> stats();
}
