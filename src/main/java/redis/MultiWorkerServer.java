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
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import redis.decode.Request;
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
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofPrimaryServer;
import static redis.ThreadFactoryAssignSupport.requestWorkerThreadFactory;
import static redis.decode.HttpHeaderBody.*;

public class MultiWorkerServer extends Launcher {
//    static {
//        ApplicationSettings.set(ServerSocketSettings.class, "receiveBufferSize", MemSize.kilobytes(64));
//        ApplicationSettings.set(SocketSettings.class, "receiveBufferSize", MemSize.kilobytes(64));
//    }

    private final int PORT = 7379;

    public static final String PROPERTIES_FILE = "redis-vlog.properties";

    private static final int MAX_NET_WORKERS = 32;

    ConfigConverter<Integer> toInt = ofInteger();

    @Inject
    PrimaryServer primaryServer;

    @Inject
    ChunkMerger chunkMerger;

    @Inject
    RequestHandler[] requestHandlerArray;

    @Inject
    Eventloop[] requestEventloopArray;

    private Eventloop firstNetWorkerEventloop;

    Eventloop[] netWorkerEventloopArray;
    HashMap<Long, Integer> netWorkerWorkerIdByThreadId = new HashMap<>();

    private static boolean reuseNetWorkers = false;

    record WrapRequestHandlerArray(RequestHandler[] requestHandlerArray) {
    }

    record WrapEventloopArray(Eventloop[] multiSlotEventloopArray) {
    }

    @Inject
    WrapRequestHandlerArray multiSlotMultiThreadRequestHandlersArray;

    @Inject
    WrapEventloopArray multiSlotMultiThreadRequestEventloopArray;

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

        if (firstNetWorkerEventloop == null) {
            firstNetWorkerEventloop = netHandleEventloop;
        }
        netWorkerEventloopArray[workerId] = netHandleEventloop;

        return netHandleEventloop;
    }

    @Provides
    WorkerPool workerPool(WorkerPools workerPools, Config config) {
        int netWorkers = config.get(toInt, "netWorkers", 1);

        netWorkerEventloopArray = new Eventloop[netWorkers];

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

    private ByteBuf handleRequest(Request request, int slotNumber, ITcpSocket socket) throws ExecutionException, InterruptedException {
        request.setSlotNumber((short) slotNumber);
        RequestHandler.parseSlots(request);

        if (reuseNetWorkers) {
            var targetHandler = requestHandlerArray[0];

            var reply = targetHandler.handle(request, socket);
            if (request.isHttp()) {
                return wrapHttpResponse(reply);
            } else {
                return reply.buffer();
            }
        }

        boolean isCrossRequestWorker = false;
        var slotWithKeyHashList = request.getSlotWithKeyHashList();
        // cross slots
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
                    isCrossRequestWorker = true;
                    break;
                }
            }
        }
        request.setCrossRequestWorker(isCrossRequestWorker);

        if (isCrossRequestWorker) {
            RequestHandler targetHandler;
            Eventloop multiSlotEventloop;

            var requestHandlerArray = multiSlotMultiThreadRequestHandlersArray.requestHandlerArray;
            var multiSlotEventloopArray = multiSlotMultiThreadRequestEventloopArray.multiSlotEventloopArray;
            if (requestHandlerArray.length == 1) {
                targetHandler = requestHandlerArray[0];
                multiSlotEventloop = multiSlotEventloopArray[0];
            } else {
                var i = new Random().nextInt(requestHandlerArray.length);
                targetHandler = requestHandlerArray[i];
                multiSlotEventloop = multiSlotEventloopArray[i];
            }

//            var netWorkerThreadId = Thread.currentThread().threadId();
//            var netWorkerWorkerId = netWorkerWorkerIdByThreadId.get(netWorkerThreadId);
//            var netWorkerEventloop = netWorkerEventloopArray[netWorkerWorkerId];

            var future = multiSlotEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket)));
//            future.whenComplete((reply, e) -> {
//                if (e != null) {
//                    logger.error("Multi slot multi thread handle request error", e);
////                    netWorkerEventloop.submit(AsyncComputation.of(() -> {
//                    socket.close();
////                    }));
//                    return;
//                }
//
//                if (reply == null) {
//                    socket.close();
//                    return;
//                }
//
//                var buffer = request.isHttp() ? wrapHttpResponse(reply) : reply.buffer();
//                socket.write(buffer);
//            });

            var reply = future.get();
            if (reply == null) {
                return null;
            }

            if (request.isHttp()) {
                return wrapHttpResponse(reply);
            } else {
                return reply.buffer();
            }
        }

        var slot = request.getSingleSlot();
        // slot == -1 means net worker can handle it
        if (slot == -1) {
            RequestHandler targetHandler;
            if (requestHandlerArray.length == 1) {
                targetHandler = requestHandlerArray[0];
            } else {
                var i = new Random().nextInt(requestHandlerArray.length);
                targetHandler = requestHandlerArray[i];
            }
            var reply = targetHandler.handle(request, socket);
            if (request.isHttp()) {
                return wrapHttpResponse(reply);
            } else {
                return reply.buffer();
            }
        }

        int i = slot % requestHandlerArray.length;
        var targetHandler = requestHandlerArray[i];
        var requestEventloop = requestEventloopArray[i];

        // use mapAsync, SettableFuture better, but exception happened, to be fixed, todo
        var future = requestEventloop.submit(AsyncComputation.of(() -> targetHandler.handle(request, socket)));
//        future.whenComplete((reply, e) -> {
//            if (e != null) {
//                logger.error("Target slot thread handle request error", e);
//                socket.close();
//                return;
//            }
//
//            if (reply == null) {
//                socket.close();
//                return;
//            }
//
//            var buffer = request.isHttp() ? wrapHttpResponse(reply) : reply.buffer();
//            socket.write(buffer);
//        });

        var reply = future.get();
        if (reply == null) {
            return null;
        }

        if (request.isHttp()) {
            return wrapHttpResponse(reply);
        } else {
            return reply.buffer();
        }
    }

    @Provides
    @Worker
    SimpleServer workerServer(NioReactor reactor, SocketInspector socketInspector, Config config) {
        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);

        return SimpleServer.builder(reactor, socket ->
                        BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
                                .decodeStream(new RequestDecoder())
                                .map(pipeline -> {
                                    if (pipeline == null) {
                                        return null;
                                    }

                                    if (pipeline.size() == 1) {
                                        return handleRequest(pipeline.get(0), slotNumber, socket);
                                    }

                                    var multiArrays = new byte[pipeline.size()][];
                                    int totalN = 0;
                                    for (int i = 0; i < pipeline.size(); i++) {
                                        var buf = handleRequest(pipeline.get(i), slotNumber, socket);
                                        if (buf != null) {
                                            multiArrays[i] = buf.array();
                                            totalN += multiArrays[i].length;
                                        }
                                    }

                                    var multiBuf = ByteBuf.wrapForWriting(new byte[totalN]);
                                    for (int i = 0; i < multiArrays.length; i++) {
                                        var array = multiArrays[i];
                                        if (array != null) {
                                            multiBuf.write(array);
                                        }
                                    }
                                    return multiBuf;
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
    ChunkMerger chunkMerger(SnowFlake snowFlake, Config config) {
        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);

        int requestWorkers = config.get(toInt, "requestWorkers", 1);
        int mergeWorkers = config.get(toInt, "mergeWorkers", slotNumber);
        int topMergeWorkers = config.get(toInt, "topMergeWorkers", slotNumber);

        var chunkMerger = new ChunkMerger((byte) requestWorkers, (byte) mergeWorkers, (byte) topMergeWorkers,
                (short) slotNumber, snowFlake);
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

    private void eventloopAsScheduler(Eventloop requestHandleEventloop, int index) {
        var taskRunnable = scheduleRunnableArray[index];
        taskRunnable.setRequestHandleEventloop(requestHandleEventloop);
        taskRunnable.setRequestHandler(requestHandlerArray[index]);

        taskRunnable.chargeOneSlots(LocalPersist.getInstance().oneSlots());

        // interval 1s
        requestHandleEventloop.delay(1000L, taskRunnable);
    }

    @Override
    protected void onStart() throws Exception {
        var localPersist = LocalPersist.getInstance();
        localPersist.initChunkMerger(chunkMerger);
        localPersist.persistMergeSegmentsUndone();

        chunkMerger.setCompressLevel(requestHandlerArray[0].compressLevel);
        chunkMerger.start();

        if (reuseNetWorkers) {
            logger.warn("Net workers and request workers are both 1, no need to create request workers");
            eventloopAsScheduler(firstNetWorkerEventloop, 0);
        } else {
            long[] threadIds = new long[requestEventloopArray.length];

            for (int i = 0; i < requestEventloopArray.length; i++) {
                var requestHandleEventloop = requestEventloopArray[i];
                eventloopAsScheduler(requestHandleEventloop, i);

                var thread = requestWorkerThreadFactory.newThread(requestHandleEventloop);
                thread.start();

                threadIds[i] = thread.getId();
            }
            logger.info("Request handle eventloop threads started");

            // fix slot thread id
            int slotNumber = configInject.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);
            for (int slot = 0; slot < slotNumber; slot++) {
                int i = slot % requestHandlerArray.length;
                localPersist.fixSlotThreadId((byte) slot, threadIds[i]);
            }

            // cross request worker threads, need use this eventloop
            // init
            var multiSlotEventloopArray = multiSlotMultiThreadRequestEventloopArray.multiSlotEventloopArray;
            for (int i = 0; i < multiSlotEventloopArray.length; i++) {
                var multiSlotEventloop = multiSlotEventloopArray[i];

                var thread = requestWorkerThreadFactory.newThread(multiSlotEventloop);
                thread.start();
            }
            logger.info("Multi slot multi thread eventloop threads started");

            for (int i = 0; i < netWorkerEventloopArray.length; i++) {
                final int finalI = i;
                var f = netWorkerEventloopArray[i].submit(AsyncComputation.of(() -> {
                    long threadId = Thread.currentThread().getId();
                    netWorkerWorkerIdByThreadId.put(threadId, finalI);
                    return threadId;
                }));
                var threadId = f.get();
                logger.info("Net worker thread id get, threadId: {}, workerId: {}", threadId, finalI);
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
            for (var requestHandler : multiSlotMultiThreadRequestHandlersArray.requestHandlerArray) {
                requestHandler.stop();
            }

            for (var requestHandler : requestHandlerArray) {
                requestHandler.stop();
            }

            for (var scheduleRunnable : scheduleRunnableArray) {
                scheduleRunnable.stop();
            }

            for (var multiSlotEventloop : multiSlotMultiThreadRequestEventloopArray.multiSlotEventloopArray) {
                multiSlotEventloop.breakEventloop();
            }

            for (var requestHandleEventloop : requestEventloopArray) {
                requestHandleEventloop.breakEventloop();
            }

            // need stop chunk merge threads first
            chunkMerger.stop();

            // disconnect all clients
            socketInspector.closeAll();

            // close local persist
            LocalPersist.getInstance().cleanUp();
            DictMap.getInstance().close();
        } catch (Exception e) {
            logger.error("Stop error", e);
            throw e;
        }

        socketInspector.clearAll();
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

                        if (config.hasChild("pureMemory")) {
                            c.pureMemory = config.get(ofBoolean(), "pureMemory");
                        }

                        int idleMillis = config.get(toInt, "eventloop.idleMillis", 10);
                        c.eventLoopIdleMillis = idleMillis;

                        boolean debugMode = config.get(ofBoolean(), "debugMode", false);
                        if (debugMode) {
                            c.confBucket.bucketsPerSlot = ConfForSlot.ConfBucket.debugMode.bucketsPerSlot;

                            c.confChunk.segmentNumberPerFd = ConfForSlot.ConfChunk.debugMode.segmentNumberPerFd;
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
                        if (config.getChild("chunk.segmentNumberPerFd").hasValue()) {
                            c.confChunk.segmentNumberPerFd = config.get(ofInteger(), "chunk.segmentNumberPerFd");
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
                        if (config.getChild("chunk.lru.maxSize").hasValue()) {
                            c.confChunk.lru.maxSize = config.get(ofInteger(), "chunk.lru.maxSize");
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

                        if (config.getChild("big.string.lru.maxSize").hasValue()) {
                            c.lruBigString.maxSize = config.get(ofInteger(), "big.string.lru.maxSize");
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
                        confForSlot.slotNumber = (short) slotNumber;

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
                        confForSlot.allWorkers = allWorkers;

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
                    RequestHandler[] requestHandlerArray(SnowFlake snowFlake, ChunkMerger chunkMerger,
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
                        var requestHandleEventloopArray = new Eventloop[requestWorkers];
                        for (int i = 0; i < requestWorkers; i++) {
                            var requestHandleEventloop = Eventloop.builder()
                                    .withThreadName("request-" + i)
                                    .withIdleInterval(Duration.ofMillis(ConfForSlot.global.eventLoopIdleMillis))
                                    .build();
                            requestHandleEventloop.keepAlive(true);

                            requestHandleEventloopArray[i] = requestHandleEventloop;
                        }
                        return requestHandleEventloopArray;
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
                    WrapRequestHandlerArray multiSlotMultiThreadRequestHandlersArray(Integer beforeCreateHandler, Config config) {
                        int slotNumber = config.get(toInt, "slotNumber", (int) LocalPersist.DEFAULT_SLOT_NUMBER);

                        int multiSlotMultiThreadNumber = config.get(toInt, "multiSlotMultiThreadNumber", 1);

                        var list = new RequestHandler[multiSlotMultiThreadNumber];
                        for (int i = 0; i < multiSlotMultiThreadNumber; i++) {
                            list[i] = new RequestHandler((byte) -i, (byte) multiSlotMultiThreadNumber, (byte) 1, (byte) 1,
                                    (short) slotNumber, null, null, config, null);
                        }
                        return new WrapRequestHandlerArray(list);
                    }

                    @Provides
                    WrapEventloopArray multiSlotMultiThreadRequestEventloopArray(Integer beforeCreateHandler, Config config) {
                        if (reuseNetWorkers) {
                            return new WrapEventloopArray(new Eventloop[0]);
                        }

                        int multiSlotMultiThreadNumber = config.get(toInt, "multiSlotMultiThreadNumber", 1);
                        var multiSlotEventloopArray = new Eventloop[multiSlotMultiThreadNumber];
                        for (int i = 0; i < multiSlotMultiThreadNumber; i++) {
                            var multiSlotEventloop = Eventloop.builder()
                                    .withThreadName("multi-slot-multi-thread-" + i)
                                    .withIdleInterval(Duration.ofMillis(ConfForSlot.global.eventLoopIdleMillis))
                                    .build();
                            multiSlotEventloop.keepAlive(true);

                            multiSlotEventloopArray[i] = multiSlotEventloop;
                        }
                        return new WrapEventloopArray(multiSlotEventloopArray);
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
