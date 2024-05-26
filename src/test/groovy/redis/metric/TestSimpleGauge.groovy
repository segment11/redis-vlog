package redis.metric

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat

def g = new SimpleGauge('test', 'test', 'slot')
def labelValues = ['0']
def labelValues2 = ['1']

g.register()
g.addRawGetter {
    Map<String, SimpleGauge.ValueWithLabelValues> map = [:]
    map.put('a', new SimpleGauge.ValueWithLabelValues(1.0, labelValues))
    map.put('b', new SimpleGauge.ValueWithLabelValues(2.0, labelValues2))
    map
}

def sw = new StringWriter()
TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples())
println sw.toString()
