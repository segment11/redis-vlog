package redis.metric

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

println ViewSupport.format()
