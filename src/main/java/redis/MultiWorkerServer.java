package redis;

import io.activej.async.callback.AsyncComputation;
import io.activej.async.function.AsyncSupplier;
import io.activej.bytebuf.ByteBuf;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.config.converter.ConfigConverter;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.launchers.initializers.Initializers;
import io.activej.net.PrimaryServer;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.BufferPoolsExports;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import redis.decode.Request;
import redis.decode.RequestDecoder;
import redis.persist.KeyBucket;
import redis.persist.LocalPersist;
import redis.persist.Wal;
import redis.reply.AsyncReply;
import redis.reply.ErrorReply;
import redis.reply.NilReply;
import redis.reply.Reply;
import redis.task.TaskRunnable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;
import static redis.decode.HttpHeaderBody.*;
import static redis.decode.Request.SLOT_CAN_HANDLE_BY_ANY_WORKER;

public class MultiWorkerServer extends Launcher {
//    static {
//        ApplicationSettings.set(ServerSocketSettings.class, "receiveBufferSize", MemSize.kilobytes(64));
//        ApplicationSettings.set(SocketSettings.class, "receiveBufferSize", MemSize.kilobytes(64));
//    }

    private final int PORT = 7379;

    public static final String PROPERTIES_FILE = "redis-vlog.properties";

    private static final int MAX_NET_WORKERS = 128;

    ConfigConverter<Integer> toInt = ofInteger();

    @Inject
    PrimaryServer primaryServer;

    @Inject
    RequestHandler[] requestHandlerArray;

    Eventloop[] netWorkerEventloopArray;

    @Inject
    SocketInspector socketInspector;

    File dirFile(Config config) {
        var dir = config.get(ofString(), "dir", "/tmp/redis-vlog");
        var dirFile = new File(dir);
        if (!dirFile.exists()) {
            boolean isOk = dirFile.mkdirs();
            logger.info("Create dir {}: {}", dirFile, isOk);
            if (!isOk) {
                throw new RuntimeException("Create dir " + dirFile.getAbsolutePath() + " failed");
            }
        }
        var persistDir = new File(dirFile, "persist");
        if (!persistDir.exists()) {
            boolean isOk = persistDir.mkdirs();
            logger.info("Create dir {}: {}", persistDir, isOk);
            if (!isOk) {
                throw new RuntimeException("Create dir " + persistDir.getAbsolutePath() + " failed");
            }
        }
        return dirFile;
    }

    @Provides
    NioReactor primaryReactor(Config config) {
        // default 10ms
        return Eventloop.builder()
                .withThreadName("primary")
                .withIdleInterval(Duration.ofMillis(ConfForSlot.global.eventLoopIdleMillis))
                .initialize(Initializers.ofEventloop(config.getChild("eventloop.primary")))
                .build();
    }

    @Provides
    @Worker
    NioReactor workerReactor(@WorkerId int workerId, OptionalDependency<ThrottlingController> throttlingController, Config config) {
        var netHandleEventloop = Eventloop.builder()
                .withThreadName("net-worker-" + workerId)
                .withIdleInterval(Duration.ofMillis(ConfForSlot.global.eventLoopIdleMillis))
                .initialize(ofEventloop(config.getChild("net.eventloop.worker")))
                .withInspector(throttlingController.orElse(null))
                .build();

        netWorkerEventloopArray[workerId] = netHandleEventloop;

        return netHandleEventloop;
    }

    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        int netWorkers = config.get(toInt, "netWorkers", 1);
        netWorkerEventloopArray = new Eventloop[netWorkers];

        // for unit test coverage
        if (workerPools == null) {
            return null;
        }

        // already checked in beforeCreateHandler
        return workerPools.createPool(netWorkers);
    }

    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        return PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
    }

    ByteBuf wrapHttpResponse(Reply reply) {
        var buf = reply.bufferAsHttp();
        byte[] array = buf.array();
        byte[] contentLengthBytes = String.valueOf(array.length).getBytes();

        var headerPrefix = reply instanceof ErrorReply ? HEADER_PREFIX_500 : (reply instanceof NilReply ? HEADER_PREFIX_404 : HEADER_PREFIX_200);
        var withHeaderLength = headerPrefix.length + contentLengthBytes.length + HEADER_SUFFIX.length + array.length;
        var withHeaderBytes = new byte[withHeaderLength];

        var httpBuf = ByteBuf.wrapForWriting(withHeaderBytes);
        httpBuf.write(headerPrefix);
        httpBuf.write(contentLengthBytes);
        httpBuf.write(HEADER_SUFFIX);
        httpBuf.write(array);
        return httpBuf;
    }

    Promise<ByteBuf> handleRequest(Request request, ITcpSocket socket) {
        // some cmd already set cross slot flag
        if (!request.isCrossRequestWorker()) {
            var slotWithKeyHashList = request.getSlotWithKeyHashList();
            if (slotWithKeyHashList != null && slotWithKeyHashList.size() > 1 && !request.isRepl()) {
                // check if cross threads
                int expectRequestWorkerId = -1;
                for (var slotWithKeyHash : slotWithKeyHashList) {
                    int slot = slotWithKeyHash.slot();
                    var expectRequestWorkerIdInner = slot % requestHandlerArray.length;
                    if (expectRequestWorkerId == -1) {
                        expectRequestWorkerId = expectRequestWorkerIdInner;
                    }
                    if (expectRequestWorkerId != expectRequestWorkerIdInner) {
                        request.setCrossRequestWorker(true);
                        break;
                    }
                }
            }
        }

        var firstSlot = request.getSingleSlot();
        if (firstSlot == SLOT_CAN_HANDLE_BY_ANY_WORKER) {
            RequestHandler targetHandler;
            if (requestHandlerArray.length == 1) {
                targetHandler = requestHandlerArray[0];
            } else {
                var i = new Random().nextInt(requestHandlerArray.length);
                targetHandler = requestHandlerArray[i];
            }
            var reply = targetHandler.handle(request, socket);
            if (reply instanceof AsyncReply) {
                var promise = ((AsyncReply) reply).getSettablePromise();
                return promise.map(r -> request.isHttp() ? wrapHttpResponse(r) : r.buffer());
            } else {
                return request.isHttp() ? Promise.of(wrapHttpResponse(reply)) : Promise.of(reply.buffer());
            }
        }

        int i = firstSlot % requestHandlerArray.length;
        var targetHandler = requestHandlerArray[i];

        var currentThreadId = Thread.currentThread().threadId();
        if (currentThreadId == netWorkerThreadIds[i]) {
            return getByteBufPromiseByCurrentEventloop(request, socket, targetHandler);
        } else {
            var otherNetWorkerEventloop = netWorkerEventloopArray[i];
            return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, otherNetWorkerEventloop);
        }
    }

    private Promise<ByteBuf> getByteBufPromiseByOtherEventloop(Request request, ITcpSocket socket, RequestHandler targetHandler,
                                                               Eventloop targetEventloop) {
        var p = targetEventloop == null ? Promises.first(AsyncSupplier.of(() -> targetHandler.handle(request, socket))) :
                Promise.ofFuture(targetEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket))));

        return p.then(reply -> {
            if (reply == null) {
                return null;
            }
            if (reply instanceof AsyncReply) {
                return ((AsyncReply) reply).getSettablePromise().map(r -> request.isHttp() ? wrapHttpResponse(r) : r.buffer());
            } else {
                return Promise.of(request.isHttp() ? wrapHttpResponse(reply) : reply.buffer());
            }
        });
    }

    private Promise<ByteBuf> getByteBufPromiseByCurrentEventloop(Request request, ITcpSocket socket, RequestHandler targetHandler) {
        return getByteBufPromiseByOtherEventloop(request, socket, targetHandler, null);
    }

    Promise<ByteBuf> handlePipeline(ArrayList<Request> pipeline, ITcpSocket socket, short slotNumber) {
        if (pipeline == null) {
            return Promise.of(null);
        }

        for (var request : pipeline) {
            request.setSlotNumber(slotNumber);
            RequestHandler.parseSlots(request);

            request.checkCmdIfCrossRequestWorker();
        }

        if (pipeline.size() == 1) {
            return handleRequest(pipeline.get(0), socket);
        }

        Promise<ByteBuf>[] promiseN = new Promise[pipeline.size()];
        for (int i = 0; i < pipeline.size(); i++) {
            var promiseI = handleRequest(pipeline.get(i), socket);
            promiseN[i] = promiseI;
        }

        return Promises.toArray(ByteBuf.class, promiseN)
                .map(bufs -> {
                    int totalN = 0;
                    for (var buf : bufs) {
                        totalN += buf.readRemaining();
                    }
                    var multiBuf = ByteBuf.wrapForWriting(new byte[totalN]);
                    for (var buf : bufs) {
                        multiBuf.put(buf);
                    }
                    return multiBuf;
                });
    }

    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Config config) {
        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
        return SimpleServer.builder(reactor, socket ->
                        BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                                .decodeStream(new RequestDecoder())
                                .mapAsync(pipeline -> handlePipeline(pipeline, socket, (short) slotNumber))
                                .streamTo(ChannelConsumers.ofSocket(socket)))
                .withSocketInspector(socketInspector)
                .build();
    }

    @Provides
    Config config() {
        return Config.create()
                .with("net.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT)))
                .overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
                .overrideWith(ofSystemProperties("redis-vlog-config"));
    }

    // no share
    // if support transaction, need share to generate lsn for all slots
    @Provides
    SnowFlake[] snowFlakes(Config config) {
        int netWorkers = config.get(toInt, "netWorkers", 1);
        var snowFlakes = new SnowFlake[netWorkers];

        long datacenterId = config.get(ofLong(), "datacenterId", 0L);
        long machineId = config.get(ofLong(), "machineId", 0L);
        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(datacenterId, (machineId << 8) | i);
        }
        return snowFlakes;
    }

    @Override
    protected final Module getModule() {
        return combine(
                ServiceGraphModule.create(),
                WorkerPoolModule.create(),
                ConfigModule.builder()
                        .withEffectiveConfigLogger()
                        .build(),
                getBusinessLogicModule()
        );
    }

    protected Module getBusinessLogicModule() {
        return Module.empty();
    }

    @Inject
    Config configInject;

    @Inject
    TaskRunnable[] scheduleRunnableArray;

    private void eventloopAsScheduler(Eventloop netWorkerEventloop, int index) {
        var taskRunnable = scheduleRunnableArray[index];
        taskRunnable.setNetWorkerEventloop(netWorkerEventloop);
        taskRunnable.setRequestHandler(requestHandlerArray[index]);

        taskRunnable.chargeOneSlots(LocalPersist.getInstance().oneSlots());

        // interval 1s
        netWorkerEventloop.delay(1000L, taskRunnable);
    }

    private long[] netWorkerThreadIds;

    @Override
    protected void onStart() throws Exception {
        var localPersist = LocalPersist.getInstance();
        localPersist.persistMergeSegmentsUndone();

        netWorkerThreadIds = new long[netWorkerEventloopArray.length];

        for (int i = 0; i < netWorkerEventloopArray.length; i++) {
            var netWorkerEventloop = netWorkerEventloopArray[i];
            netWorkerThreadIds[i] = netWorkerEventloop.getEventloopThread().threadId();

            // start schedule
            eventloopAsScheduler(netWorkerEventloop, i);
        }
        logger.info("Net worker eventloop scheduler started");

        // fix slot thread id
        int slotNumber = configInject.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
        for (int slot = 0; slot < slotNumber; slot++) {
            int i = slot % requestHandlerArray.length;
            localPersist.fixSlotThreadId((byte) slot, netWorkerThreadIds[i]);
        }

        localPersist.setSocketInspector(socketInspector);

        // metrics
        CollectorRegistry.defaultRegistry.register(new BufferPoolsExports());
        CollectorRegistry.defaultRegistry.register(new MemoryPoolsExports());
        CollectorRegistry.defaultRegistry.register(new GarbageCollectorExports());
        logger.info("Prometheus jvm hotspot metrics registered");
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    @Override
    protected void onStop() throws Exception {
        try {
            for (var requestHandler : requestHandlerArray) {
                requestHandler.stop();
            }

            for (var scheduleRunnable : scheduleRunnableArray) {
                scheduleRunnable.stop();
            }

            if (socketInspector != null) {
                socketInspector.socketMap.values().forEach(socket -> {
                    socket.getReactor().execute(() -> {
                        socket.close();
                        logger.info("Close connected socket: {}", socket.getRemoteAddress());
                    });
                });
            }

            // close local persist
            LocalPersist.getInstance().cleanUp();
            DictMap.getInstance().close();
        } catch (Exception e) {
            logger.error("Stop error", e);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        Launcher launcher = new MultiWorkerServer() {
            @Override
            protected Module getBusinessLogicModule() {
                return new AbstractModule() {
                    @Provides
                    ConfForSlot confForSlot(Config config) {
                        long estimateKeyNumber = config.get(ofLong(), "estimateKeyNumber", 1_000_000L);
                        int estimateOneValueLength = config.get(toInt, "estimateOneValueLength", 200);
                        var c = ConfForSlot.from(estimateKeyNumber);
                        c.estimateOneValueLength = estimateOneValueLength;

                        boolean isValueSetUseCompression = config.get(ofBoolean(), "isValueSetUseCompression", true);
                        boolean isOnDynTrainDictForCompression = config.get(ofBoolean(), "isOnDynTrainDictForCompression", true);
                        c.isValueSetUseCompression = isValueSetUseCompression;
                        c.isOnDynTrainDictForCompression = isOnDynTrainDictForCompression;

                        int toCompressMinDataLength = config.get(toInt, "toCompressMinDataLength", 64);
                        DictMap.TO_COMPRESS_MIN_DATA_LENGTH = toCompressMinDataLength;

                        ConfForSlot.global = c;

                        c.netListenAddresses = config.get(ofString(), "net.listenAddresses");
                        logger.info("Net listen addresses: {}", c.netListenAddresses);

                        if (config.hasChild("pureMemory")) {
                            c.pureMemory = config.get(ofBoolean(), "pureMemory");
                        }

                        c.eventLoopIdleMillis = config.get(toInt, "eventloop.idleMillis", 10);

                        boolean debugMode = config.get(ofBoolean(), "debugMode", false);
                        if (debugMode) {
                            c.confBucket.bucketsPerSlot = ConfForSlot.ConfBucket.debugMode.bucketsPerSlot;
                            c.confBucket.initialSplitNumber = ConfForSlot.ConfBucket.debugMode.initialSplitNumber;

                            c.confChunk.segmentNumberPerFd = ConfForSlot.ConfChunk.debugMode.segmentNumberPerFd;
                            c.confChunk.fdPerChunk = ConfForSlot.ConfChunk.debugMode.fdPerChunk;
                            c.confChunk.segmentLength = ConfForSlot.ConfChunk.debugMode.segmentLength;

                            c.confWal.oneChargeBucketNumber = ConfForSlot.ConfWal.debugMode.oneChargeBucketNumber;
                            c.confWal.valueSizeTrigger = ConfForSlot.ConfWal.debugMode.valueSizeTrigger;
                            c.confWal.shortValueSizeTrigger = ConfForSlot.ConfWal.debugMode.shortValueSizeTrigger;
                        }

                        var debugInstance = Debug.getInstance();
                        debugInstance.logMerge = config.get(ofBoolean(), "debugLogMerge", false);
                        debugInstance.logTrainDict = config.get(ofBoolean(), "debugLogTrainDict", false);
                        debugInstance.logRestore = config.get(ofBoolean(), "debugLogRestore", false);
                        debugInstance.bulkLoad = config.get(ofBoolean(), "bulkLoad", false);

                        // override bucket conf
                        if (config.getChild("bucket.bucketsPerSlot").hasValue()) {
                            c.confBucket.bucketsPerSlot = config.get(ofInteger(), "bucket.bucketsPerSlot");
                        }
                        if (c.confBucket.bucketsPerSlot > KeyBucket.MAX_BUCKETS_PER_SLOT) {
                            throw new IllegalArgumentException("Bucket count per slot too large, bucket count per slot should be less than " + KeyBucket.MAX_BUCKETS_PER_SLOT);
                        }
                        if (c.confBucket.bucketsPerSlot % 1024 != 0) {
                            throw new IllegalArgumentException("Bucket count per slot should be multiple of 1024");
                        }
                        if (config.getChild("bucket.initialSplitNumber").hasValue()) {
                            c.confBucket.initialSplitNumber = config.get(ofInteger(), "bucket.initialSplitNumber").byteValue();
                        }

                        if (config.getChild("bucket.lruPerFd.maxSize").hasValue()) {
                            c.confBucket.lruPerFd.maxSize = config.get(toInt, "bucket.lruPerFd.maxSize");
                        }

                        // override chunk conf
                        if (config.getChild("chunk.segmentNumberPerFd").hasValue()) {
                            c.confChunk.segmentNumberPerFd = config.get(ofInteger(), "chunk.segmentNumberPerFd");
                        }
                        if (config.getChild("chunk.fdPerChunk").hasValue()) {
                            c.confChunk.fdPerChunk = Byte.parseByte(config.get("chunk.fdPerChunk"));
                        }
                        if (c.confChunk.fdPerChunk > ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK) {
                            throw new IllegalArgumentException("Chunk fd per chunk too large, fd per chunk should be less than " + ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK);
                        }

                        if (config.getChild("chunk.segmentLength").hasValue()) {
                            c.confChunk.segmentLength = config.get(ofInteger(), "chunk.segmentLength");
                        }
                        if (config.getChild("chunk.lruPerFd.maxSize").hasValue()) {
                            c.confChunk.lruPerFd.maxSize = config.get(ofInteger(), "chunk.lruPerFd.maxSize");
                        }

                        c.confChunk.resetByOneValueLength(estimateOneValueLength);

                        // override wal conf
                        if (config.getChild("wal.oneChargeBucketNumber").hasValue()) {
                            c.confWal.oneChargeBucketNumber = config.get(ofInteger(), "wal.oneChargeBucketNumber");
                        }
                        if (!Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST.contains(c.confWal.oneChargeBucketNumber)) {
                            throw new IllegalArgumentException("Wal one charge bucket number invalid, wal one charge bucket number should be in " + Wal.VALID_ONE_CHARGE_BUCKET_NUMBER_LIST);
                        }

                        var walGroupNumber = c.confBucket.bucketsPerSlot / c.confWal.oneChargeBucketNumber;
                        if (walGroupNumber > Wal.MAX_WAL_GROUP_NUMBER) {
                            throw new IllegalArgumentException("Wal group number too large, wal group number should be less than " + Wal.MAX_WAL_GROUP_NUMBER);
                        }

                        if (config.getChild("wal.valueSizeTrigger").hasValue()) {
                            c.confWal.valueSizeTrigger = config.get(ofInteger(), "wal.valueSizeTrigger");
                        }
                        if (config.getChild("wal.shortValueSizeTrigger").hasValue()) {
                            c.confWal.shortValueSizeTrigger = config.get(ofInteger(), "wal.shortValueSizeTrigger");
                        }

                        c.confWal.resetByOneValueLength(estimateOneValueLength);

                        if (config.getChild("repl.binlogForReadCacheSegmentMaxCount").hasValue()) {
                            c.confRepl.binlogForReadCacheSegmentMaxCount = config.get(ofInteger(), "repl.binlogForReadCacheSegmentMaxCount").shortValue();
                        }

                        if (config.getChild("big.string.lru.maxSize").hasValue()) {
                            c.lruBigString.maxSize = config.get(ofInteger(), "big.string.lru.maxSize");
                        }
                        if (config.getChild("kv.lru.maxSize").hasValue()) {
                            c.lruKeyAndCompressedValueEncoded.maxSize = config.get(ofInteger(), "kv.lru.maxSize");
                        }

                        logger.info("ConfForSlot: {}", c);
                        return c;
                    }

                    @Provides
                    Integer beforeCreateHandler(ConfForSlot confForSlot, SnowFlake[] snowFlakes, Config config) throws IOException {
                        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
                        if (slotNumber > LocalPersist.MAX_SLOT_NUMBER) {
                            throw new IllegalArgumentException("Slot number too large, slot number should be less than " + LocalPersist.MAX_SLOT_NUMBER);
                        }
                        if (slotNumber != 1 && slotNumber % 2 != 0) {
                            throw new IllegalArgumentException("Slot number should be 1 or even");
                        }
                        confForSlot.slotNumber = (short) slotNumber;

                        int netWorkers = config.get(toInt, "netWorkers", 1);
                        if (netWorkers > MAX_NET_WORKERS) {
                            throw new IllegalArgumentException("Net workers too large, net workers should be less than " + MAX_NET_WORKERS);
                        }
                        var cpuNumber = Runtime.getRuntime().availableProcessors();
                        if (netWorkers >= cpuNumber) {
                            throw new IllegalArgumentException("Net workers should be less than cpu number");
                        }
                        confForSlot.netWorkers = (byte) netWorkers;

                        var dirFile = dirFile(config);

                        DictMap.getInstance().initDictMap(dirFile);

                        var configCompress = config.getChild("compress");
                        TrainSampleJob.setDictKeyPrefixEndIndex(configCompress.get(toInt, "dictKeyPrefixEndIndex", 5));

                        // init local persist
                        // already created when inject
                        var persistDir = new File(dirFile, "persist");
                        LocalPersist.getInstance().initSlots((byte) netWorkers, (short) slotNumber, snowFlakes, persistDir,
                                config.getChild("persist"));

                        boolean debugMode = config.get(ofBoolean(), "debugMode", false);
                        if (debugMode) {
                            LocalPersist.getInstance().debugMode();
                        }

                        return 0;
                    }

                    @Provides
                    RequestHandler[] requestHandlerArray(SnowFlake[] snowFlakes, Integer beforeCreateHandler, Config config) {
                        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
                        int netWorkers = config.get(toInt, "netWorkers", 1);

                        var list = new RequestHandler[netWorkers];
                        for (int i = 0; i < netWorkers; i++) {
                            list[i] = new RequestHandler((byte) i, (byte) netWorkers, (short) slotNumber, snowFlakes[i], config);
                        }
                        return list;
                    }

                    @Provides
                    TaskRunnable[] scheduleRunnableArray(Integer beforeCreateHandler, Config config) {
                        int netWorkers = config.get(toInt, "netWorkers", 1);
                        var list = new TaskRunnable[netWorkers];
                        for (int i = 0; i < netWorkers; i++) {
                            list[i] = new TaskRunnable((byte) i, (byte) netWorkers);
                        }
                        return list;
                    }

                    @Provides
                    SocketInspector socketInspector(Config config) {
                        int maxConnections = config.get(toInt, "maxConnections", 1000);

                        var r = new SocketInspector();
                        r.setMaxConnections(maxConnections);
                        MultiWorkerServer.staticGlobalV.socketInspector = r;
                        return r;
                    }
                };
            }
        };
        launcher.launch(args);
    }

    public static class StaticGlobalV {
        public SocketInspector socketInspector;
    }

    public static StaticGlobalV staticGlobalV = new StaticGlobalV();
}
