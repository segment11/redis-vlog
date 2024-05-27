package redis.metric

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat

class ViewSupport {
    static String format() {
        def sw = new StringWriter()
        TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples())
        sw.toString()
    }
}
