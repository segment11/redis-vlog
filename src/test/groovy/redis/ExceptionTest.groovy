package redis

import redis.persist.BucketFullException
import redis.persist.ReadonlyException
import redis.persist.SegmentOverflowException
import spock.lang.Specification

class ExceptionTest extends Specification {
    // just for coverage
    def 'test constructor'() {
        when:
        new DictMissingException()
        new TypeMismatchException('xxx')
        new BucketFullException('xxx')
        new SegmentOverflowException('xxx')
        new ReadonlyException()

        then:
        1 == 1
    }
}
