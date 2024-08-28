package redis.command

import com.github.luben.zstd.Zstd
import groovy.transform.CompileStatic
import io.activej.promise.Promise
import io.activej.promise.Promises
import io.activej.promise.SettablePromise
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import redis.BaseCommand
import redis.ConfForSlot
import redis.Debug
import redis.TrainSampleJob
import redis.persist.Chunk
import redis.persist.OneSlot
import redis.repl.support.JedisPoolHolder
import redis.reply.*
import redis.type.RedisHH
import redis.type.RedisHashKeys
import redis.type.RedisList
import redis.type.RedisZSet

import static redis.TrainSampleJob.MIN_TRAIN_SAMPLE_SIZE

@CompileStatic
class ManageCommand extends BaseCommand {
    static final String version = '1.0.1'

    ManageCommand(MGroup mGroup) {
        super(mGroup.cmd, mGroup.data, mGroup.socket)
    }

    static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> r = []

        if (data.length < 2) {
            return r
        }

        def subCmd = new String(data[1])
        // manage slot 0 bucket 0 view-key-count
        if (subCmd == 'slot') {
            if (data.length < 5) {
                return r
            }

            def slotBytes = data[2]
            short slot
            try {
                slot = Short.parseShort(new String(slotBytes))
            } catch (NumberFormatException ignored) {
                return r
            }

            r.add(new SlotWithKeyHash(slot, 0, 0L))
            return r
        }

        r
    }

    @Override
    Reply handle() {
        log.info 'Dyn manage command version: {}', version

        if (data.length < 2) {
            return ErrorReply.FORMAT
        }

        def subCmd = new String(data[1])

        // cross slots
        if (subCmd == 'debug') {
            return debug()
        }

        // cross slots
        if (subCmd == 'dyn-config') {
            return dynConfig()
        }

        // cross slots
        if (subCmd == 'dict') {
            return dict()
        }

        // given slot
        if (subCmd == 'slot') {
            return manageInOneSlot()
        }

        return NilReply.INSTANCE
    }

    Reply manageInOneSlot() {
        if (data.length < 4) {
            return ErrorReply.FORMAT
        }

        def slotBytes = data[2]
        short slot

        try {
            slot = Short.parseShort(new String(slotBytes))
        } catch (NumberFormatException ignored) {
            return ErrorReply.INVALID_INTEGER
        }

        int bucketIndex = -1

        int subSubCmdIndex = 3
        def isInspectBucket = 'bucket' == new String(data[3])
        if (isInspectBucket) {
            def bucketIndexBytes = data[4]

            try {
                bucketIndex = Integer.parseInt(new String(bucketIndexBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.INVALID_INTEGER
            }

            subSubCmdIndex = 5

            if (data.length < 6) {
                return ErrorReply.FORMAT
            }
        }

        def oneSlot = localPersist.oneSlot(slot)

        def subSubCmd = new String(data[subSubCmdIndex])
        if (subSubCmd == 'view-metrics') {
            // http url: ?manage&slot&0&view-metrics
            def metricsCollectedMap = OneSlot.oneSlotGauge.rawGetterList.find {
                it.slot() == slot
            }.get()

            // prometheus format
            def sb = new StringBuilder()
            metricsCollectedMap.each { k, v ->
                sb << k << '{slot="' << slot << '",} ' << v.value() << '\n'
            }
            return new BulkReply(sb.toString().bytes)
        } else if (subSubCmd == 'view-bucket-key-count') {
            // manage slot 0 view-bucket-key-count
            // manage slot 0 bucket 0 view-bucket-key-count
            def keyCount = bucketIndex == -1 ? oneSlot.getAllKeyCount() : oneSlot.keyLoader.getKeyCountInBucketIndex(bucketIndex)
            return new IntegerReply(keyCount)
        } else if (subSubCmd == 'view-bucket-keys') {
            // manage slot 0 bucket 0 view-bucket-keys [iterate]
            def isIterate = data.length == subSubCmdIndex + 2 && new String(data[data.length - 1]) == 'iterate'

            // if not set bucket index, default 0
            if (bucketIndex == -1) {
                bucketIndex = 0
            }

            def keyBuckets = oneSlot.keyLoader.readKeyBuckets(bucketIndex)
            String str
            if (!isIterate) {
                str = keyBuckets.collect { it == null ? 'Null' : it.toString() }.join(',')
            } else {
                def sb = new StringBuilder()
                for (kb in keyBuckets) {
                    if (kb == null) {
                        continue
                    }
                    kb.iterate { keyHash, expireAt, seq, keyBytes, valueBytes ->
                        sb << new String(keyBytes) << ','
                    }
                }
                str = sb.toString()
            }

            return new BulkReply(str.bytes)
        } else if (subSubCmd == 'update-kv-lru-max-size') {
            // manage slot 0 update-kv-lru-max-size 100
            if (data.length != 5) {
                return ErrorReply.FORMAT
            }

            def lruMaxSizeBytes = data[4]

            int lruMaxSize

            try {
                lruMaxSize = Integer.parseInt(new String(lruMaxSizeBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize = lruMaxSize
            oneSlot.initLRU(true)

            return OKReply.INSTANCE
        } else if (subSubCmd == 'view-in-memory-size-estimate') {
            return new IntegerReply(oneSlot.estimate())
        } else if (subSubCmd == 'output-chunk-segment-flag-to-file') {
            // manage slot 0 output-chunk-segment-flag-to-file 0 1024
            if (data.length != 6) {
                return ErrorReply.FORMAT
            }

            def beginSegmentIndexBytes = data[4]
            def segmentCountBytes = data[5]
            int beginSegmentIndex
            int segmentCount

            try {
                beginSegmentIndex = Integer.parseInt(new String(beginSegmentIndexBytes))
                segmentCount = Integer.parseInt(new String(segmentCountBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.INVALID_INTEGER
            }

            if (beginSegmentIndex < 0) {
                return ErrorReply.SYNTAX
            }

            def maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber()
            if (beginSegmentIndex >= maxSegmentNumber) {
                return new ErrorReply('begin segment index need less than ' + maxSegmentNumber)
            }

            def isIterateAll = segmentCount <= 0

            def outputDir = new File(oneSlot.slotDir, 'debug')
            FileUtils.forceMkdir(outputDir)

            if (isIterateAll) {
                final String outputFileName = 'chunk_segment_flag.txt'
                new File(outputDir, outputFileName).withWriter { writer ->
                    writer.writeLine Chunk.Flag.values().collect { it.name() + ':' + it.flagByte() }.join(',')
                    oneSlot.metaChunkSegmentFlagSeq.iterateAll { segmentIndex, flag, seq, walGroupIndex ->
                        writer.writeLine("$segmentIndex, $flag, $seq, $walGroupIndex")
                    }
                }
            } else {
                final String outputFileName = 'chunk_segment_flag_range.txt'
                new File(outputDir, outputFileName).withWriter { writer ->
                    writer.writeLine Chunk.Flag.values().collect { it.name() + ':' + it.flagByte() }.join(',')
                    oneSlot.metaChunkSegmentFlagSeq.iterateRange(beginSegmentIndex, segmentCount) { segmentIndex, flag, seq, walGroupIndex ->
                        writer.writeLine("$segmentIndex, $flag, $seq, $walGroupIndex")
                    }
                }
            }

            return OKReply.INSTANCE
        } else if (subSubCmd == 'set-readonly') {
            oneSlot.readonly = true
            return new BulkReply(('slot ' + slot + ' set readonly').bytes)
        } else if (subSubCmd == 'set-not-readonly') {
            oneSlot.readonly = false
            return new BulkReply(('slot ' + slot + ' set not readonly').bytes)
        } else if (subSubCmd == 'set-can-read') {
            oneSlot.canRead = true
            return new BulkReply(('slot ' + slot + ' set can read').bytes)
        } else if (subSubCmd == 'set-not-can-read') {
            oneSlot.canRead = false
            return new BulkReply(('slot ' + slot + ' set not can read').bytes)
        }

        return ErrorReply.SYNTAX
    }

    private Reply trainSampleListAndReturnRatio(String keyPrefixOrSuffixGiven, List<TrainSampleJob.TrainSampleKV> sampleToTrainList) {
        def trainSampleJob = new TrainSampleJob(workerId)
        trainSampleJob.resetSampleToTrainList(sampleToTrainList)
        def trainSampleResult = trainSampleJob.train()

        // only one key prefix given, only one dict after train
        def trainSampleCacheDict = trainSampleResult.cacheDict()
        def onlyOneDict = trainSampleCacheDict.get(keyPrefixOrSuffixGiven)
        log.warn 'Train new dict result, sample value count: {}, dict count: {}', data.length - 4, trainSampleCacheDict.size()
        // will overwrite same key prefix dict exists
        dictMap.putDict(keyPrefixOrSuffixGiven, onlyOneDict)

//            def oldDict = dictMap.putDict(keyPrefixOrSuffixGiven, onlyOneDict)
//            if (oldDict != null) {
//                // keep old dict in persist, because may be used by other worker
//                // when start server, early dict will be overwritten by new dict with same key prefix, need not persist again?
//                dictMap.putDict(keyPrefixOrSuffixGiven + '_' + new Random().nextInt(10000), oldDict)
//            }

        // show compress ratio use dict just trained
        long totalBytes = 0
        long totalCompressedBytes = 0
        for (sample in sampleToTrainList) {
            totalBytes += sample.valueBytes().length
            def compressedBytes = Zstd.compressUsingDict(sample.valueBytes(), onlyOneDict.dictBytes, Zstd.defaultCompressionLevel())
            totalCompressedBytes += compressedBytes.length
        }

        def ratio = totalCompressedBytes / totalBytes
        return new BulkReply(ratio.toString().bytes)
    }

    Reply dict() {
        if (data.length < 3) {
            return ErrorReply.FORMAT
        }

        def subSubCmd = new String(data[2])
        if (subSubCmd == 'set-key-prefix-groups') {
            // manage dict set-key-prefix-groups keyPrefix1,keyPrefix2
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def keyPrefixGroups = new String(data[3])
            if (!keyPrefixGroups) {
                return ErrorReply.SYNTAX
            }

            var firstOneSlot = localPersist.currentThreadFirstOneSlot()
            firstOneSlot.dynConfig.update("dict_key_prefix_groups", keyPrefixGroups);

            return OKReply.INSTANCE
        }

        if (subSubCmd == 'view-dict-summary') {
            // manage dict view-dict-summary
            if (data.length != 3) {
                return ErrorReply.FORMAT
            }

            def sb = new StringBuilder()
            dictMap.cacheDictBySeqCopy.each { seq, dict ->
                sb << dict << '\n'
            }
            sb << '----------------\n'
            dictMap.cacheDictCopy.each { keyPrefix, dict ->
                sb << keyPrefix << ': ' << dict << '\n'
            }

            return new BulkReply(sb.toString().bytes)
        }

        if (subSubCmd == 'train-new-dict') {
            // manage dict train-new-dict keyPrefixOrSuffix sampleValue1 sampleValue2 ...
            if (data.length <= 4 + MIN_TRAIN_SAMPLE_SIZE) {
                return new ErrorReply('Train sample value count too small')
            }

            def keyPrefixOrSuffixGiven = new String(data[3])

            List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
            for (int i = 4; i < data.length; i++) {
                sampleToTrainList << new TrainSampleJob.TrainSampleKV(null, keyPrefixOrSuffixGiven, 0L, data[i])
            }

            return trainSampleListAndReturnRatio(keyPrefixOrSuffixGiven, sampleToTrainList)
        }

        if (subSubCmd == 'train-new-dict-by-keys-in-redis') {
            // manage dict train-new-dict-by-keys-in-redis keyPrefixOrSuffix host port sampleKey1 sampleKey2
            if (data.length <= 6 + MIN_TRAIN_SAMPLE_SIZE) {
                return new ErrorReply('Train sample value count too small')
            }

            def keyPrefixOrSuffixGiven = new String(data[3])
            def host = new String(data[4])
            def portBytes = data[5]

            int port
            try {
                port = Integer.parseInt(new String(portBytes))
            } catch (NumberFormatException e) {
                return ErrorReply.INVALID_INTEGER
            }

            def log = LoggerFactory.getLogger(ManageCommand.class)
            byte[][] valueBytesArray = new byte[data.length - 6][]
            try {
                def jedisPool = JedisPoolHolder.instance.create(host, port)
                // may be null
                JedisPoolHolder.exe(jedisPool, jedis -> {
                    def pong = jedis.ping()
                    log.info("Manage train dict, remove redis server: {}:{} pong: {}", host, port, pong);
                })

                /*
                set a aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                hset b f1 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                hset b f2 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                hset b f3 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                lpush c aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                lpush c aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                lpush c aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd
                sadd d aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd0 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd1 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd2
                zadd e 1 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd0 2 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd1 3 aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd2
                 */
                JedisPoolHolder.exe(jedisPool, jedis -> {
                    for (i in 6..<data.length) {
                        def key = new String(data[i])
                        def isExists = jedis.exists(key)
                        if (isExists) {
                            def type = jedis.type(key)
                            if (type == 'string') {
                                valueBytesArray[i - 6] = jedis.get(key)?.bytes
                            } else if (type == 'list') {
                                def list = jedis.lrange(key, 0, -1)
                                def rl = new RedisList()
                                for (one in list) {
                                    rl.addLast(one.bytes)
                                }
                                valueBytesArray[i - 6] = rl.encodeButDoNotCompress()
                            } else if (type == 'hash') {
                                def hash = jedis.hgetAll(key)
                                def rhh = new RedisHH()
                                hash.each { k, v ->
                                    rhh.put(k, v.bytes)
                                }
                                valueBytesArray[i - 6] = rhh.encodeButDoNotCompress()
                            } else if (type == 'set') {
                                def set = jedis.smembers(key)
                                def rhk = new RedisHashKeys()
                                for (one in set) {
                                    rhk.add(one)
                                }
                                valueBytesArray[i - 6] = rhk.encodeButDoNotCompress()
                            } else if (type == 'zset') {
                                def zset = jedis.zrandmemberWithScores(key, 1000)
                                def rz = new RedisZSet()
                                for (one in zset) {
                                    rz.add(one.score, new String(one.element))
                                }
                                valueBytesArray[i - 6] = rz.encodeButDoNotCompress()
                            }
                        }
                    }
                    null
                })
            } catch (Exception e) {
                return new ErrorReply(e.message)
            }

            List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
            for (i in 0..<valueBytesArray.length) {
                if (valueBytesArray[i] == null) {
                    return new ErrorReply('Key not exists or type not support: ' + new String(data[i + 6]))
                }
                sampleToTrainList << new TrainSampleJob.TrainSampleKV(null, keyPrefixOrSuffixGiven, 0L, valueBytesArray[i])
            }

            return trainSampleListAndReturnRatio(keyPrefixOrSuffixGiven, sampleToTrainList)
        }

        if (subSubCmd == 'output-dict-bytes') {
            // manage dict output-dict-bytes 12345
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def dictSeqBytes = data[3]
            int dictSeq

            try {
                dictSeq = Integer.parseInt(new String(dictSeqBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            def dict = dictMap.getDictBySeq(dictSeq)
            if (dict == null) {
                return new ErrorReply('Dict not found, dict seq: ' + dictSeq)
            }

            def userHome = System.getProperty('user.home')
            def file = new File(new File(userHome), 'dict-seq-' + dictSeq + '.dat')
            try {
                file.bytes = dict.dictBytes
                log.info 'Output dict bytes to file: {}', file.absolutePath
            } catch (IOException e) {
                return new ErrorReply(e.message)
            }

            return OKReply.INSTANCE
        }

        return ErrorReply.SYNTAX
    }

    Reply dynConfig() {
        // manage dyn-config key value
        if (data.length != 4) {
            return ErrorReply.FORMAT
        }

        def configKeyBytes = data[2]
        def configValueBytes = data[3]

        def configKey = new String(configKeyBytes)

        ArrayList<Promise<Boolean>> promises = []
        def oneSlots = localPersist.oneSlots()
        for (oneSlot in oneSlots) {
            def p = oneSlot.asyncCall(() -> oneSlot.updateDynConfig(configKey, configValueBytes))
            promises.add(p)
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error 'Manage dyn-config set error: {}', e.message
                finalPromise.exception = e
                return
            }

            // every true
            for (int i = 0; i < promises.size(); i++) {
                def p = promises.get(i)
                if (!p.result) {
                    finalPromise.set(new ErrorReply('Slot ' + i + ' set dyn-config failed'))
                    return
                }
            }

            finalPromise.set(OKReply.INSTANCE)
        })

        return asyncReply
    }

    Reply debug() {
        if (data.length < 4) {
            return ErrorReply.FORMAT
        }

        def subSubCmd = new String(data[2])
        if (subSubCmd == 'log-switch') {
            if (data.length != 5) {
                return ErrorReply.FORMAT
            }

            // manage debug log-switch logCmd 1
            def field = new String(data[3])
            def val = new String(data[4])
            def isOn = val == '1' || val == 'true'

            switch (field) {
                case 'logCmd' -> Debug.getInstance().logCmd = isOn
                case 'logMerge' -> Debug.getInstance().logMerge = isOn
                case 'logTrainDict' -> Debug.getInstance().logTrainDict = isOn
                case 'logRestore' -> Debug.getInstance().logRestore = isOn
                case 'bulkLoad' -> Debug.getInstance().bulkLoad = isOn
                default -> {
                    log.warn 'Manage unknown debug field: {}', field
                }
            }

            return OKReply.INSTANCE
        } else if (subSubCmd == 'calc-key-hash') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            // manage debug calc-key-hash key
            def keyBytes = data[3]
            def slotWithKeyHash = slot(keyBytes)
            return new BulkReply(slotWithKeyHash.toString().bytes)
        }

        return ErrorReply.SYNTAX
    }
}