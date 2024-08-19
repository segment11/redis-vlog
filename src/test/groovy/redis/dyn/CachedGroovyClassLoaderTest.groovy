package redis.dyn

import spock.lang.Specification

class CachedGroovyClassLoaderTest extends Specification {
    def 'parse class'() {
        given:
        def loader = CachedGroovyClassLoader.instance
        loader.init(null, './', null)

        def clz = loader.gcl.parseClass(new File('dyn/src/Test.groovy'))
        def clz2 = loader.gcl.parseClass(new File('dyn/src/Test.groovy'))
        println clz
        println clz2
        def obj = clz.newInstance()

        expect:
        clz == clz2
        obj.hi() == 'hi kerry'
        loader.eval('"hi"') == 'hi'
        loader.eval('"hi ${name}"', [name: 'kerry']) == 'hi kerry'
    }
}
