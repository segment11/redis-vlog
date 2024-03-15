package redis;

import io.activej.async.callback.AsyncComputation;
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
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import redis.decode.RequestDecoder;
import redis.persist.ChunkMerger;
import redis.persist.KeyBucket;
import redis.persist.LocalPersist;
import redis.persist.Wal;
import redis.reply.ErrorReply;
import redis.reply.NilReply;
import redis.reply.Reply;
import redis.task.TaskRunnable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ThreadFactory;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;
import static redis.decode.HttpHeaderBody.*;

public class MultiWorkerServer extends Launcher {
    private final int PORT = 7379;

    public static final String PROPERTIES_FILE = "redis-vlog.properties";

    private static final int MAX_NET_WORKERS = 32;

    ConfigConverter<Integer> toInt = ofInteger();

    @Inject
    PrimaryServer primaryServer;

    @Inject
    ChunkMerger chunkMerger;

    @Inject
    RequestHandler[] requestHandlersArray;

    private static final ThreadFactory requestWorkerThreadFactory = new AffinityThreadFactory("request-worker",
            AffinityStrategies.DIFFERENT_CORE);

    @Inject
    Eventloop[] requestEventloopArray;

    private Eventloop firstNetWorkerEventloop;

    private static boolean reuseNetWorkers = false;

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
        int idleMillis = config.get(toInt, "idleMillis", 10);
        return Eventloop.builder()
                .withThreadName("primary")
                .withIdleInterval(Duration.ofMillis(idleMillis))
                .initialize(Initializers.ofEventloop(config.getChild("eventloop.primary")))
                .build();
    }

    @Provides
    @Worker
    NioReactor workerReactor(@WorkerId int workerId, OptionalDependency<ThrottlingController> throttlingController, Config config) {
        // default 10ms
        int idleMillis = config.get(toInt, "idleMillis", 10);
        var eventloop = Eventloop.builder()
                .withThreadName("net-worker-" + workerId)
                .withIdleInterval(Duration.ofMillis(idleMillis))
                .initialize(ofEventloop(config.getChild("eventloop.worker")))
                .withInspector(throttlingController.orElse(null))
                .build();

        if (firstNetWorkerEventloop == null) {
            firstNetWorkerEventloop = eventloop;
        }
        return eventloop;
    }

    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        int netWorkers = config.get(toInt, "netWorkers", 1);
        // already checked in beforeCreateHandler
        return workerPools.createPool(netWorkers);
    }

    @Provides
    PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<SimpleServer> workerServers, Config config) {
        var ps = PrimaryServer.builder(primaryReactor, workerServers.getList())
                .initialize(ofPrimaryServer(config.getChild("net")))
                .build();
        return ps;
    }

    private ByteBuf wrapHttpResponse(Reply reply) {
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

    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Config config) {
        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);

        return SimpleServer.builder(reactor, socket ->
                        BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                                .decodeStream(new RequestDecoder())
                                .map(request -> {
                                    if (request == null) {
                                        return null;
                                    }

                                    if (requestHandlersArray.length > 1) {
                                        request.setSlotNumber((short) slotNumber);
                                        RequestHandler.parseSlot(request);
                                    }

                                    var slot = request.getSlot();
                                    if (slot == -1) {
                                        RequestHandler targetHandler;
                                        if (requestHandlersArray.length == 1) {
                                            targetHandler = requestHandlersArray[0];
                                        } else {
                                            var i = new Random().nextInt(requestHandlersArray.length);
                                            targetHandler = requestHandlersArray[i];
                                        }
                                        var reply = targetHandler.handle(request, socket);
                                        if (request.isHttp()) {
                                            return wrapHttpResponse(reply);
                                        } else {
                                            return reply.buffer();
                                        }
                                    }

                                    // need not submit to request workers
                                    if (reuseNetWorkers) {
                                        var targetHandler = requestHandlersArray[0];

                                        var reply = targetHandler.handle(request, socket);
                                        if (request.isHttp()) {
                                            return wrapHttpResponse(reply);
                                        } else {
                                            return reply.buffer();
                                        }
                                    }

                                    int i = slot % requestHandlersArray.length;
                                    var targetHandler = requestHandlersArray[i];
                                    var eventloop = requestEventloopArray[i];

                                    // use mapAsync, SettableFuture better, but exception happened, to be fixed, todo

                                    var future = eventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket)));
                                    var reply = future.get();
                                    // repl handle may return null
                                    if (reply == null) {
                                        return null;
                                    }

                                    if (request.isHttp()) {
                                        return wrapHttpResponse(reply);
                                    } else {
                                        return reply.buffer();
                                    }
                                })
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

    @Provides
    SnowFlake snowFlake(Config config) {
        long datacenterId = config.get(ofLong(), "datacenterId", 0L);
        long machineId = config.get(ofLong(), "machineId", 0L);
        return new SnowFlake(datacenterId, machineId);
    }

    @Provides
    ChunkMerger chunkMerger(SnowFlake snowFlake, Config config) throws IOException {
        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);

        int requestWorkers = config.get(toInt, "requestWorkers", 1);
        int mergeWorkers = config.get(toInt, "mergeWorkers", slotNumber);
        int topMergeWorkers = config.get(toInt, "topMergeWorkers", slotNumber);

        var chunkConfig = config.getChild("chunk");
        var chunkMerger = new ChunkMerger((byte) requestWorkers, (byte) mergeWorkers, (byte) topMergeWorkers,
                (short) slotNumber, snowFlake, chunkConfig);
        return chunkMerger;
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

    private void eventloopAsScheduler(Eventloop eventloop, int index) {
        var taskRunnable = scheduleRunnableArray[index];
        taskRunnable.setEventloop(eventloop);
        taskRunnable.setRequestHandler(requestHandlersArray[index]);

        taskRunnable.chargeOneSlots(LocalPersist.getInstance().oneSlots());

        // interval 1s
        eventloop.delay(1000L, taskRunnable);
    }

    @Override
    protected void onStart() throws Exception {
        var localPersist = LocalPersist.getInstance();
        localPersist.initChunkMerger(chunkMerger);
        localPersist.persistMergeSegmentsUndone();

        chunkMerger.setCompressLevel(requestHandlersArray[0].compressLevel);
        chunkMerger.start();

        if (reuseNetWorkers) {
            logger.warn("Net workers and request workers are both 1, no need to create request workers");
            eventloopAsScheduler(firstNetWorkerEventloop, 0);
        } else {
            long[] threadIds = new long[requestEventloopArray.length];

            for (int i = 0; i < requestEventloopArray.length; i++) {
                var eventloop = requestEventloopArray[i];
                eventloopAsScheduler(eventloop, i);

                var thread = requestWorkerThreadFactory.newThread(eventloop);
                thread.start();

                threadIds[i] = thread.getId();
            }
            logger.info("Request eventloop threads started");

            // fix slot thread id
            int slotNumber = configInject.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
            for (int slot = 0; slot < slotNumber; slot++) {
                int i = slot % requestHandlersArray.length;
                localPersist.fixSlotThreadId((byte) slot, threadIds[i]);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    @Override
    protected void onStop() throws Exception {
        try {
            for (var requestHandler : requestHandlersArray) {
                requestHandler.stop();
            }

            for (var scheduleRunnable : scheduleRunnableArray) {
                scheduleRunnable.stop();
            }

            for (var eventloop : requestEventloopArray) {
                eventloop.breakEventloop();
            }

            // need stop chunk merge threads first
            chunkMerger.stop();

            // disconnect all clients
            // todo

            // close local persist
            LocalPersist.getInstance().cleanUp();
            DictMap.getInstance().close();
        } catch (Exception e) {
            logger.error("Stop error", e);
            throw e;
        }

        // close all clients
        // todo
//        for (var socket : socketInspector.socketMap.values()) {
//            socket.close();
//        }
        socketInspector.socketMap.clear();
    }

    public static void main(String[] args) throws Exception {
        Launcher launcher = new MultiWorkerServer() {
            @Override
            protected Module getBusinessLogicModule() {
                return new AbstractModule() {
                    @Provides
                    ConfForSlot confForSlot(Config config) {
                        long estimateKeyNumber = config.get(ofLong(), "estimateKeyNumber", 1_000_000L);
                        var c = ConfForSlot.from(estimateKeyNumber);
                        ConfForSlot.global = c;

                        c.netListenAddresses = config.get(ofString(), "net.listenAddresses");
                        logger.info("Net listen addresses: {}", c.netListenAddresses);

                        boolean debugMode = config.get(ofBoolean(), "debugMode", false);
                        if (debugMode) {
                            c.confBucket.bucketsPerSlot = ConfForSlot.ConfBucket.debugMode.bucketsPerSlot;

                            c.confChunk.segmentPower2 = ConfForSlot.ConfChunk.debugMode.segmentPower2;
                            c.confChunk.fdPerChunk = ConfForSlot.ConfChunk.debugMode.fdPerChunk;

                            c.confWal.oneChargeBucketNumber = ConfForSlot.ConfWal.debugMode.oneChargeBucketNumber;
                        }

                        // override bucket conf
                        if (config.getChild("bucket.bucketsPerSlot").hasValue()) {
                            c.confBucket.bucketsPerSlot = config.get(ofInteger(), "bucket.bucketsPerSlot");
                        }
                        if (c.confBucket.bucketsPerSlot > KeyBucket.MAX_BUCKETS_PER_SLOT) {
                            throw new IllegalStateException("Bucket count per slot too large, bucket count per slot should be less than " + KeyBucket.MAX_BUCKETS_PER_SLOT);
                        }
                        if (c.confBucket.bucketsPerSlot % 1024 != 0) {
                            throw new IllegalStateException("Bucket count per slot should be multiple of 1024");
                        }

                        if (config.getChild("bucket.lru.expireAfterWrite").hasValue()) {
                            c.confBucket.lru.expireAfterWrite = config.get(ofLong(), "bucket.lru.expireAfterWrite");
                        }
                        if (config.getChild("bucket.lru.expireAfterAccess").hasValue()) {
                            c.confBucket.lru.expireAfterAccess = config.get(ofLong(), "bucket.lru.expireAfterAccess");
                        }
                        if (config.getChild("bucket.lru.maximumBytes").hasValue()) {
                            c.confBucket.lru.maximumBytes = config.get(ofLong(), "bucket.lru.maximumBytes");
                        }

                        // override chunk conf
                        if (config.getChild("chunk.segmentPower2").hasValue()) {
                            c.confChunk.segmentPower2 = config.get(ofInteger(), "chunk.segmentPower2");
                        }
                        if (config.getChild("chunk.fdPerChunk").hasValue()) {
                            c.confChunk.fdPerChunk = Byte.parseByte(config.get("chunk.fdPerChunk"));
                        }
                        if (config.getChild("chunk.segmentLength").hasValue()) {
                            c.confChunk.segmentLength = config.get(ofInteger(), "chunk.segmentLength");
                        }

                        if (config.getChild("chunk.lru.expireAfterWrite").hasValue()) {
                            c.confChunk.lru.expireAfterWrite = config.get(ofLong(), "chunk.lru.expireAfterWrite");
                        }
                        if (config.getChild("chunk.lru.expireAfterAccess").hasValue()) {
                            c.confChunk.lru.expireAfterAccess = config.get(ofLong(), "chunk.lru.expireAfterAccess");
                        }
                        if (config.getChild("chunk.lru.maximumBytes").hasValue()) {
                            c.confChunk.lru.maximumBytes = config.get(ofLong(), "chunk.lru.maximumBytes");
                        }

                        // override wal conf
                        if (config.getChild("wal.oneChargeBucketNumber").hasValue()) {
                            c.confWal.oneChargeBucketNumber = config.get(ofInteger(), "wal.oneChargeBucketNumber");
                        }

                        var walGroupNumber = c.confBucket.bucketsPerSlot / c.confWal.oneChargeBucketNumber;
                        if (walGroupNumber > Wal.MAX_WAL_GROUP_NUMBER) {
                            throw new IllegalStateException("Wal group number too large, wal group number should be less than " + Wal.MAX_WAL_GROUP_NUMBER);
                        }

                        if (config.getChild("wal.batchNumber").hasValue()) {
                            c.confWal.batchNumber = config.get(ofInteger(), "wal.batchNumber");
                        }
                        if (config.getChild("wal.valueSizeTrigger").hasValue()) {
                            c.confWal.valueSizeTrigger = config.get(ofInteger(), "wal.valueSizeTrigger");
                        }
                        if (config.getChild("wal.shortValueSizeTrigger").hasValue()) {
                            c.confWal.shortValueSizeTrigger = config.get(ofInteger(), "wal.shortValueSizeTrigger");
                        }

                        logger.info("ConfForSlot: {}", c);
                        return c;
                    }

                    @Provides
                    Integer beforeCreateHandler(ConfForSlot confForSlot, SnowFlake snowFlake, Config config) throws IOException {
                        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
                        if (slotNumber > LocalPersist.MAX_SLOT_NUMBER) {
                            throw new IllegalStateException("Slot number too large, slot number should be less than " + LocalPersist.MAX_SLOT_NUMBER);
                        }
                        if (slotNumber != 1 && slotNumber % 2 != 0) {
                            throw new IllegalStateException("Slot number should be 1 or even");
                        }

                        int netWorkers = config.get(toInt, "netWorkers", 1);
                        int requestWorkers = config.get(toInt, "requestWorkers", 1);
                        int mergeWorkers = config.get(toInt, "mergeWorkers", slotNumber);
                        int topMergeWorkers = config.get(toInt, "topMergeWorkers", slotNumber);

                        if (netWorkers > MAX_NET_WORKERS) {
                            throw new IllegalStateException("Net workers too large, net workers should be less than " + MAX_NET_WORKERS);
                        }
                        if (requestWorkers > RequestHandler.MAX_REQUEST_WORKERS) {
                            throw new IllegalStateException("Request workers too large, request workers should be less than " + RequestHandler.MAX_REQUEST_WORKERS);
                        }
                        if (mergeWorkers > ChunkMerger.MAX_MERGE_WORKERS) {
                            throw new IllegalStateException("Merge workers too large, merge workers should be less than " + ChunkMerger.MAX_MERGE_WORKERS);
                        }
                        if (topMergeWorkers > ChunkMerger.MAX_TOP_MERGE_WORKERS) {
                            throw new IllegalStateException("Top merge workers too large, top merge workers should be less than " + ChunkMerger.MAX_TOP_MERGE_WORKERS);
                        }

                        reuseNetWorkers = netWorkers == 1 && requestWorkers == 1;

                        // not include net workers
                        byte allWorkers = (byte) (requestWorkers + mergeWorkers + topMergeWorkers);

                        logger.info("netWorkers: {}, requestWorkers: {}, mergeWorkers: {}, topMergeWorkers: {}, slotNumber: {}",
                                netWorkers, requestWorkers, mergeWorkers, topMergeWorkers, slotNumber);

                        var dirFile = dirFile(config);

                        DictMap.getInstance().initDictMap(dirFile);

                        var configCompress = config.getChild("compress");
                        TrainSampleJob.setDictPrefixKeyMaxLen(configCompress.get(toInt, "dictPrefixKeyMaxLen", 5));

                        // init local persist
                        LocalPersist.getInstance().init(allWorkers, (byte) netWorkers, (byte) requestWorkers, (byte) mergeWorkers, (byte) topMergeWorkers, (short) slotNumber,
                                snowFlake, dirFile, config.getChild("persist"));

                        boolean debugMode = config.get(ofBoolean(), "debugMode", false);
                        if (debugMode) {
                            LocalPersist.getInstance().debugMode();
                        }

                        return 0;
                    }

                    @Provides
                    RequestHandler[] requestHandlersArray(SnowFlake snowFlake, ChunkMerger chunkMerger,
                                                          Integer beforeCreateHandler, SocketInspector socketInspector, Config config) {
                        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);

                        int requestWorkers = config.get(toInt, "requestWorkers", 1);
                        int mergeWorkers = config.get(toInt, "mergeWorkers", slotNumber);
                        int topMergeWorkers = config.get(toInt, "topMergeWorkers", slotNumber);

                        var list = new RequestHandler[requestWorkers];
                        for (int i = 0; i < requestWorkers; i++) {
                            list[i] = new RequestHandler((byte) i, (byte) requestWorkers, (byte) mergeWorkers, (byte) topMergeWorkers,
                                    (short) slotNumber, snowFlake, chunkMerger, config, socketInspector);
                        }
                        return list;
                    }

                    @Provides
                    Eventloop[] requestEventloopArray(Integer beforeCreateHandler, Config config) {
                        if (reuseNetWorkers) {
                            return new Eventloop[0];
                        }

                        int requestWorkers = config.get(toInt, "requestWorkers", 1);
                        var list = new Eventloop[requestWorkers];
                        for (int i = 0; i < requestWorkers; i++) {
                            int idleMillis = config.get(toInt, "eventloop.idleMillis", 10);
                            var eventloop = Eventloop.builder()
                                    .withThreadName("request-" + i)
                                    .withIdleInterval(Duration.ofMillis(idleMillis))
                                    .build();
                            eventloop.keepAlive(true);

                            list[i] = eventloop;
                        }
                        return list;
                    }

                    @Provides
                    TaskRunnable[] scheduleRunnableArray(Integer beforeCreateHandler, Config config) {
                        int requestWorkers = config.get(toInt, "requestWorkers", 1);
                        var list = new TaskRunnable[requestWorkers];
                        for (int i = 0; i < requestWorkers; i++) {
                            list[i] = new TaskRunnable((byte) i, (byte) requestWorkers);
                        }
                        return list;
                    }

                    @Provides
                    SocketInspector socketInspector() {
                        return new SocketInspector();
                    }
                };
            }
        };
        launcher.launch(args);
    }
}
