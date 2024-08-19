package redis.command

import groovy.transform.CompileStatic
import io.activej.promise.Promise
import io.activej.promise.Promises
import io.activej.promise.SettablePromise
import redis.BaseCommand
import redis.ConfForSlot
import redis.Debug
import redis.reply.*

@CompileStatic
class ManageCommand extends BaseCommand {
    static final String version = '1.0.0'

    ManageCommand(MGroup mGroup) {
        super(mGroup.cmd, mGroup.data, mGroup.socket)
    }

    static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> r = []

        if (data.length < 2) {
            return r
        }

        def subCmd = new String(data[1])
        if (subCmd in ['view-slot-bucket-key-count', 'view-slot-bucket-keys', 'update-kv-lru-max-size']) {
            if (data.length != 4) {
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

        if (subCmd == 'output-dict-bytes') {
            if (data.length != 4) {
                return r
            }

            def keyBytes = data[3]
            r << slot(keyBytes, slotNumber)
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

        if (subCmd == 'debug') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def field = new String(data[2])
            def val = new String(data[3])
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
        }

        if (subCmd == 'set-dict-key-prefix-groups') {
            if (data.length != 3) {
                return ErrorReply.FORMAT
            }

            def keyPrefixGroups = new String(data[2])
            if (!keyPrefixGroups) {
                return ErrorReply.SYNTAX
            }

            ArrayList<String> keyPrefixGroupList = []
            for (keyPrefixGroup in keyPrefixGroups.split(',')) {
                if (keyPrefixGroup) {
                    return ErrorReply.SYNTAX
                }
                keyPrefixGroupList << keyPrefixGroup
            }

            trainSampleJob.keyPrefixGroupList = keyPrefixGroupList
            log.warn 'Manage Set dict key prefix groups: {}', keyPrefixGroups
            return OKReply.INSTANCE
        }

        if (subCmd == 'view-slot-bucket-key-count') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def slotBytes = data[2]
            def bucketIndexBytes = data[3]

            byte slot
            int bucketIndex

            try {
                slot = Byte.parseByte(new String(slotBytes))
                bucketIndex = Integer.parseInt(new String(bucketIndexBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            def oneSlot = localPersist.oneSlot(slot)

            def keyCount = bucketIndex == -1 ? oneSlot.getAllKeyCount() : oneSlot.keyLoader.getKeyCountInBucketIndex(bucketIndex)
            return new IntegerReply(keyCount)
        }

        if (subCmd == 'view-slot-bucket-keys') {
            if (data.length != 4 && data.length != 5) {
                return ErrorReply.FORMAT
            }

            def slotBytes = data[2]
            def bucketIndexBytes = data[3]

            byte slot
            int bucketIndex

            try {
                slot = Byte.parseByte(new String(slotBytes))
                bucketIndex = Integer.parseInt(new String(bucketIndexBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            def isIterate = data.length == 5 && new String(data[4]) == 'iterate'

            def oneSlot = localPersist.oneSlot(slot)

            def keyBuckets = oneSlot.keyLoader.readKeyBuckets(bucketIndex)
            String str
            if (!isIterate) {
                str = keyBuckets.collect { it.toString() }.join(',')
            } else {
                def sb = new StringBuilder()
                for (kb in keyBuckets) {
                    kb.iterate { keyHash, expireAt, seq, keyBytes, valueBytes ->
                        sb << new String(keyBytes) << ','
                    }
                }
                str = sb.toString()
            }

            return new BulkReply(str.bytes)
        }

        if (subCmd == 'output-dict-bytes') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def dictSeqBytes = data[2]
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
            def file = new File(new File(userHome), 'redis-d200-dict.txt')
            try {
                file.bytes = dict.dictBytes
                log.info 'Output dict bytes to file: {}', file.absolutePath
            } catch (IOException e) {
                return new ErrorReply(e.message)
            }

            def keyBytes = data[3]
            def slotWithKeyHash = slotWithKeyHashListParsed[0]
            def valueBytes = get(keyBytes, slotWithKeyHash)
            if (valueBytes == null) {
                return NilReply.INSTANCE
            }

            def file2 = new File(new File(userHome), 'redis-d200-value.txt')
            try {
                file2.bytes = valueBytes
                log.info 'Output value bytes to file: {}', file2.absolutePath
            } catch (IOException e) {
                return new ErrorReply(e.message)
            }

            return OKReply.INSTANCE
        }

        if (subCmd == 'get-slot-with-key-hash') {
            if (data.length != 3) {
                return ErrorReply.FORMAT
            }

            def keyBytes = data[2]
            def slotWithKeyHash = slot(keyBytes)
            return new BulkReply(slotWithKeyHash.toString().bytes)
        }

        if (subCmd == 'dyn-config') {
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

        // eg: manage update-kv-lru-max-size 0 100
        if (subCmd == 'update-kv-lru-max-size') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def slotBytes = data[2]
            def lruMaxSizeBytes = data[3]

            byte slot
            int lruMaxSize

            try {
                slot = Byte.parseByte(new String(slotBytes))
                lruMaxSize = Integer.parseInt(new String(lruMaxSizeBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize = lruMaxSize

            def oneSlot = localPersist.oneSlot(slot)
            oneSlot.initLRU(true)

            return OKReply.INSTANCE
        }

        return NilReply.INSTANCE
    }
}