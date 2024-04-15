package redis

import com.github.luben.zstd.Zstd
import spock.lang.Specification

class TrainSampleJobTest extends Specification {

    def 'test key prefix'() {
        given:
        TrainSampleJob.dictPrefixKeyMaxLen = 6
        TrainSampleJob.keyPrefixGroupList = ['key:']

        expect:
        TrainSampleJob.keyPrefix('key:1234567890') == 'key:'
        TrainSampleJob.keyPrefix('test.xxx.yyy') == 'test.xxx'
        TrainSampleJob.keyPrefix('test:xxx') == 'test'
        TrainSampleJob.keyPrefix('xxxyyyzzz') == 'xxxyyy'
    }

    def 'test train'() {
        given:
        def job = new TrainSampleJob((byte) 0)

        and:
        job.dictSize = 512
        job.trainSampleMinBodyLength = 1024

        and:
        final String sampleValue = 'xxxx' * 5 + 'yyyy' * 5 + 'zzzz' * 5
        final byte[] sampleValueBytes = sampleValue.bytes

        def snowFlake = new SnowFlake(0, 0)

        TrainSampleJob.keyPrefixGroupList = ['key:']
        List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
        11.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), sampleValueBytes)
        }

        job.resetSampleToTrainList(sampleToTrainList)
        def result = job.train()

        expect:
        result.cacheDict.size() == 1
        result.removedSampleKVSeqList.size() == sampleToTrainList.size()
        result.removedSampleKVSeqList == sampleToTrainList.collect { it.seq }

        when:
        def dict = result.cacheDict.get('key:')
        var cv = CompressedValue.compress(sampleValueBytes, dict, Zstd.defaultCompressionLevel())
        def decompressBytes = cv.decompress(dict)

        then:
        Arrays.equals(sampleValueBytes, decompressBytes)
    }
}
