package redis;

import com.github.luben.zstd.Zstd;
import io.activej.config.Config;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.command.*;
import redis.decode.Request;
import redis.persist.ChunkMerger;
import redis.persist.LocalPersist;
import redis.persist.ReadonlyException;
import redis.persist.SegmentOverflowException;
import redis.reply.*;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.activej.config.converter.ConfigConverters.*;

public class RequestHandler implements OfStats {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public final static byte MAX_REQUEST_WORKERS = 32;

    private static final String PING_COMMAND = "ping";
    private static final String AUTH_COMMAND = "auth";
    private static final String GET_COMMAND = "get";
    private static final String SET_COMMAND = "set";
    private static final String QUIT_COMMAND = "quit";
    private static final String STATS_COMMAND = "stats";

    private final String password;

    final byte workerId;
    final byte requestWorkers;
    final byte mergeWorkers;
    final byte topMergeWorkers;
    final short slotNumber;
    final SnowFlake snowFlake;
    private final ChunkMerger chunkMerger;
    private final SocketInspector socketInspector;
    final boolean localTest;
    final int localTestRandomValueListSize;
    final ArrayList<byte[]> localTestRandomValueList;

    private final LocalPersist localPersist = LocalPersist.getInstance();

    int compressLevel;
    int trainSampleListMaxSize;

    final CompressStats compressStats;

    final TrainSampleJob trainSampleJob;
    final List<TrainSampleJob.TrainSampleKV> sampleToTrainList = new CopyOnWriteArrayList<>();

    private volatile boolean isStopped = false;

    void stop() {
        System.out.println("Worker " + workerId + " stopped callback");
        isStopped = true;
    }

    public RequestHandler(byte workerId, byte requestWorkers, byte mergeWorkers, byte topMergeWorkers,
                          short slotNumber, SnowFlake snowFlake, ChunkMerger chunkMerger,
                          Config config, SocketInspector socketInspector) {
        this.workerId = workerId;
        this.requestWorkers = requestWorkers;
        this.mergeWorkers = mergeWorkers;
        this.topMergeWorkers = topMergeWorkers;
        this.slotNumber = slotNumber;

        this.snowFlake = snowFlake;
        this.chunkMerger = chunkMerger;
        this.socketInspector = socketInspector;

        this.password = config.get(ofString(), "password", null);

        var toInt = ofInteger();
        this.localTest = config.get(ofBoolean(), "localTest", false);
        var localTestRandomValueLength = config.get(toInt, "localTestRandomValueLength", 200);
        this.localTestRandomValueListSize = config.get(toInt, "localTestRandomValueListSize", 10000);
        this.localTestRandomValueList = new ArrayList<>(localTestRandomValueListSize);
        if (this.localTest) {
            var rand = new Random();
            for (int i = 0; i < localTestRandomValueListSize; i++) {
                var value = new byte[localTestRandomValueLength];
                for (int j = 0; j < value.length; j++) {
                    value[j] = (byte) rand.nextInt(Byte.MAX_VALUE + 1);
                }
                localTestRandomValueList.add(value);
            }
            log.info("Local test random value list mocked, size: {}, value length: {}", localTestRandomValueListSize, localTestRandomValueLength);
        }

        var requestConfig = config.getChild("request");

        this.compressStats = new CompressStats("req-w-" + workerId);
        // compress and train sample dict requestConfig
        this.compressLevel = requestConfig.get(toInt, "compressLevel", Zstd.defaultCompressionLevel());
        this.trainSampleListMaxSize = requestConfig.get(toInt, "trainSampleListMaxSize", 1000);

        this.trainSampleJob = new TrainSampleJob(workerId);
        this.trainSampleJob.setDictSize(requestConfig.get(toInt, "dictSize", 1024));
        this.trainSampleJob.setTrainSampleMinBodyLength(requestConfig.get(toInt, "trainSampleMinBodyLength", 4096));
    }

    public static void parseSlots(@NotNull Request request) {
        var cmd = request.cmd();
        if (cmd.equals(PING_COMMAND) || cmd.equals(QUIT_COMMAND) || cmd.equals(AUTH_COMMAND) || cmd.equals(STATS_COMMAND)) {
            return;
        }

        ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList = null;

        var data = request.getData();
        var firstByte = data[0][0];
        if (firstByte == 'a' || firstByte == 'A') {
            slotWithKeyHashList = AGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'b' || firstByte == 'B') {
            slotWithKeyHashList = BGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'c' || firstByte == 'C') {
            slotWithKeyHashList = CGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'd' || firstByte == 'D') {
            slotWithKeyHashList = DGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'e' || firstByte == 'E') {
            slotWithKeyHashList = EGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'f' || firstByte == 'F') {
            slotWithKeyHashList = FGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'g' || firstByte == 'G') {
            slotWithKeyHashList = GGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'h' || firstByte == 'H') {
            slotWithKeyHashList = HGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'i' || firstByte == 'I') {
            slotWithKeyHashList = IGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'j' || firstByte == 'J') {
            slotWithKeyHashList = JGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'k' || firstByte == 'K') {
            slotWithKeyHashList = KGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'l' || firstByte == 'L') {
            slotWithKeyHashList = LGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'm' || firstByte == 'M') {
            slotWithKeyHashList = MGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'n' || firstByte == 'N') {
            slotWithKeyHashList = NGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'o' || firstByte == 'O') {
            slotWithKeyHashList = OGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'p' || firstByte == 'P') {
            slotWithKeyHashList = PGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'q' || firstByte == 'Q') {
            slotWithKeyHashList = QGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'r' || firstByte == 'R') {
            slotWithKeyHashList = RGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 's' || firstByte == 'S') {
            slotWithKeyHashList = SGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 't' || firstByte == 'T') {
            slotWithKeyHashList = TGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'u' || firstByte == 'U') {
            slotWithKeyHashList = UGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'v' || firstByte == 'V') {
            slotWithKeyHashList = VGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'w' || firstByte == 'W') {
            slotWithKeyHashList = WGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'x' || firstByte == 'X') {
            slotWithKeyHashList = XGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'y' || firstByte == 'Y') {
            slotWithKeyHashList = YGroup.parseSlots(cmd, data, request.getSlotNumber());
        } else if (firstByte == 'z' || firstByte == 'Z') {
            slotWithKeyHashList = ZGroup.parseSlots(cmd, data, request.getSlotNumber());
        }

        request.setSlotWithKeyHashList(slotWithKeyHashList);
    }

    public Reply handle(@NotNull Request request, ITcpSocket socket) {
        if (isStopped) {
            return ErrorReply.SERVER_STOPPED;
        }

        var data = request.getData();

        if (request.isRepl()) {
            var xGroup = new XGroup(null, data, socket);
            xGroup.init(this, request);

            return xGroup.handleRepl();
        }

        var cmd = request.cmd();

        if (cmd.equals(PING_COMMAND)) {
            return PongReply.INSTANCE;
        }

        if (cmd.equals(QUIT_COMMAND)) {
            socket.close();
            return OKReply.INSTANCE;
        }

        InetSocketAddress remoteAddress = ((TcpSocket) socket).getRemoteAddress();
        if (cmd.equals(AUTH_COMMAND)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            if (password == null) {
                return ErrorReply.NO_PASSWORD;
            }

            if (!password.equals(new String(data[1]))) {
                return ErrorReply.AUTH_FAILED;
            }
            AuthHolder.flagBySocketAddress.put(remoteAddress, true);
            return OKReply.INSTANCE;
        }

        if (password != null && AuthHolder.flagBySocketAddress.get(remoteAddress) == null) {
            return ErrorReply.NO_AUTH;
        }

        if (cmd.equals(GET_COMMAND)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var keyBytes = data[1];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            var gGroup = new GGroup(cmd, data, socket).init(this, request);
            try {
                var slotWithKeyHashList = request.getSlotWithKeyHashList();
                var bytes = gGroup.get(keyBytes, slotWithKeyHashList.get(0), true);
                return bytes != null ? new BulkReply(bytes) : NilReply.INSTANCE;
            } catch (TypeMismatchException e) {
                return new ErrorReply(e.getMessage());
            } catch (DictMissingException e) {
                return ErrorReply.DICT_MISSING;
            }
        }

        // for short
        // full set command handle in SGroup
        if (cmd.equals(SET_COMMAND) && data.length == 3) {
            var keyBytes = data[1];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }

            // for local test, random value, test compress ratio
            var valueBytes = data[2];
            if (valueBytes.length > CompressedValue.VALUE_MAX_LENGTH) {
                return ErrorReply.VALUE_TOO_LONG;
            }

            var sGroup = new SGroup(cmd, data, socket).init(this, request);
            try {
                sGroup.set(keyBytes, valueBytes);
            } catch (ReadonlyException e) {
                return ErrorReply.READONLY;
            } catch (SegmentOverflowException e) {
                return new ErrorReply(e.getMessage());
            } catch (IllegalStateException e) {
                return new ErrorReply(e.getMessage());
            }

            return OKReply.INSTANCE;
        }

        if (cmd.equals(STATS_COMMAND)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            String subCommand = new String(data[1]);
            // extend here, todo

            byte workerIdGiven = Byte.parseByte(subCommand);
            if (workerIdGiven != -1 && workerIdGiven != workerId) {
                return NilReply.INSTANCE;
            }
            byte slotGiven = Byte.parseByte(new String(data[2]));
            if (slotGiven < 0) {
                slotGiven = 0;
            }

            // global stats show first
            var list = localPersist.oneSlot(slotGiven).stats();

            list.add(new StatKV("global last seq", snowFlake.getLastNextId()));
            list.add(new StatKV("global connected clients", socketInspector.socketMap.size()));
            list.add(StatKV.split);

            list.addAll(DictMap.getInstance().stats());

            list.addAll(stats());
            list.addAll(compressStats.stats());

            list.addAll(chunkMerger.stats());

            System.out.println(Utils.padStats(list, 60));
            return OKReply.INSTANCE;
        }

        // else, use enum better
        var firstByte = data[0][0];
        try {
            if (firstByte == 'a' || firstByte == 'A') {
                return new AGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'b' || firstByte == 'B') {
                return new BGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'c' || firstByte == 'C') {
                return new CGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'd' || firstByte == 'D') {
                return new DGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'e' || firstByte == 'E') {
                return new EGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'f' || firstByte == 'F') {
                return new FGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'g' || firstByte == 'G') {
                return new GGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'h' || firstByte == 'H') {
                return new HGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'i' || firstByte == 'I') {
                return new IGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'j' || firstByte == 'J') {
                return new JGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'k' || firstByte == 'K') {
                return new KGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'l' || firstByte == 'L') {
                return new LGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'm' || firstByte == 'M') {
                return new MGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'n' || firstByte == 'N') {
                return new NGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'o' || firstByte == 'O') {
                return new OGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'p' || firstByte == 'P') {
                return new PGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'q' || firstByte == 'Q') {
                return new QGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'r' || firstByte == 'R') {
                return new RGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 's' || firstByte == 'S') {
                return new SGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 't' || firstByte == 'T') {
                return new TGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'u' || firstByte == 'U') {
                return new UGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'v' || firstByte == 'V') {
                return new VGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'w' || firstByte == 'W') {
                return new WGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'x' || firstByte == 'X') {
                return new XGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'y' || firstByte == 'Y') {
                return new YGroup(cmd, data, socket).init(this, request).handle();
            } else if (firstByte == 'z' || firstByte == 'Z') {
                return new ZGroup(cmd, data, socket).init(this, request).handle();
            }
        } catch (ReadonlyException e) {
            return ErrorReply.READONLY;
        }

        return ErrorReply.FORMAT;
    }

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();

        final String prefix = "req-w-" + workerId + " ";
        list.add(new StatKV(prefix + "sample to train size", sampleToTrainList.size()));

        list.add(StatKV.split);
        return list;
    }
}
