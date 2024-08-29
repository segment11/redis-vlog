package redis.metric;

import java.util.Map;

public interface InSlotMetricCollector {
    Map<String, Double> collect();
}
