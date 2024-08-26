package redis.command

import groovy.transform.CompileStatic
import io.activej.promise.Promise
import io.activej.promise.Promises
import io.activej.promise.SettablePromise
import org.apache.commons.io.FileUtils
import redis.BaseCommand
import redis.ConfForSlot
import redis.Debug
import redis.TrainSampleJob
import redis.persist.Chunk
import redis.reply.*

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
            byte slot
            try {
                slot = Byte.parseByte(new String(slotBytes))
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
        byte slot

        try {
            slot = Byte.parseByte(new String(slotBytes))
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
        if (subSubCmd == 'view-bucket-key-count') {
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
            // manage dict train-new-dict keyPrefix sampleValue1 sampleValue2 ...
            if (data.length <= 4 + MIN_TRAIN_SAMPLE_SIZE) {
                return new ErrorReply('Train sample value count too small')
            }

            def keyPrefixGiven = new String(data[3])

            List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
            for (int i = 4; i < data.length; i++) {
                sampleToTrainList << new TrainSampleJob.TrainSampleKV(null, keyPrefixGiven, 0L, data[i])
            }

            def trainSampleJob = new TrainSampleJob(workerId)
            trainSampleJob.resetSampleToTrainList(sampleToTrainList)
            def trainSampleResult = trainSampleJob.train()

            def trainSampleCacheDict = trainSampleResult.cacheDict()
            log.warn 'Train new dict result, sample value count: {}, dict count: {}', data.length - 4, trainSampleCacheDict.size()
            trainSampleCacheDict.each { keyPrefix, dict ->
                // will overwrite same key prefix dict exists
                dictMap.putDict(keyPrefix, dict)
//                def oldDict = dictMap.putDict(keyPrefix, dict)
//                if (oldDict != null) {
//                    // keep old dict in persist, because may be used by other worker
//                    // when start server, early dict will be overwritten by new dict with same key prefix, need not persist again?
//                    dictMap.putDict(keyPrefix + '_' + new Random().nextInt(10000), oldDict)
//                }
            }

            return new IntegerReply(trainSampleCacheDict.size())
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