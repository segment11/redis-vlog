package redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import io.activej.config.Config;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.command.*;
import redis.decode.Request;
import redis.metric.SimpleGauge;
import redis.persist.ReadonlyException;
import redis.reply.*;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.activej.config.converter.ConfigConverters.*;

public class RequestHandler {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final String PING_COMMAND = "ping";
    private static final String AUTH_COMMAND = "auth";
    private static final String GET_COMMAND = "get";
    private static final String SET_COMMAND = "set";
    private static final String QUIT_COMMAND = "quit";

    final byte workerId;
    final String workerIdStr;
    final byte netWorkers;
    final short slotNumber;
    final SnowFlake snowFlake;
    String password;

    final boolean localTest;
    final int localTestRandomValueListSize;
    final ArrayList<byte[]> localTestRandomValueList;

    int compressLevel;
    int trainSampleListMaxSize;

    final CompressStats compressStats;

    final TrainSampleJob trainSampleJob;
    final List<TrainSampleJob.TrainSampleKV> sampleToTrainList = new CopyOnWriteArrayList<>();

    volatile boolean isStopped = false;

    void stop() {
        System.out.println("Worker " + workerId + " stopped callback");
        isStopped = true;
    }

    @Override
    public String toString() {
        return "RequestHandler{" +
                "workerId=" + workerId +
                ", netWorkers=" + netWorkers +
                ", slotNumber=" + slotNumber +
                ", localTest=" + localTest +
                ", compressLevel=" + compressLevel +
                ", sampleToTrainList.size=" + sampleToTrainList.size() +
                ", isStopped=" + isStopped +
                '}';
    }

    public RequestHandler(byte workerId, byte netWorkers, short slotNumber, SnowFlake snowFlake, Config config) {
        this.workerId = workerId;
        this.workerIdStr = String.valueOf(workerId);
        this.netWorkers = netWorkers;
        this.slotNumber = slotNumber;
        this.snowFlake = snowFlake;

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

        this.compressStats = new CompressStats("net_worker_" + workerId);
        // compress and train sample dict requestConfig
        this.compressLevel = requestConfig.get(toInt, "compressLevel", Zstd.defaultCompressionLevel());
        this.trainSampleListMaxSize = requestConfig.get(toInt, "trainSampleListMaxSize", 1000);

        this.trainSampleJob = new TrainSampleJob(workerId);
        this.trainSampleJob.setDictSize(requestConfig.get(toInt, "dictSize", 1024));
        this.trainSampleJob.setTrainSampleMinBodyLength(requestConfig.get(toInt, "trainSampleMinBodyLength", 4096));

        this.initMetricsCollect();
    }

    public static void parseSlots(@NotNull Request request) {
        var cmd = request.cmd();
        if (cmd.equals(PING_COMMAND) || cmd.equals(QUIT_COMMAND) || cmd.equals(AUTH_COMMAND)) {
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

    private static final byte[] URL_QUERY_METRICS_FIRST_PARAM_BYTES = "metrics".getBytes();

    Reply handle(@NotNull Request request, ITcpSocket socket) {
        if (isStopped) {
            return ErrorReply.SERVER_STOPPED;
        }

        var data = request.getData();

        if (request.isRepl()) {
            var xGroup = new XGroup(null, data, socket);
            xGroup.init(this, request);

            try {
                return xGroup.handleRepl();
            } catch (Exception e) {
                log.error("Repl handle error", e);
                return new ErrorReply(e.getMessage());
            }
        }

        // metrics, prometheus format
        // url should be ?metrics
        if (request.isHttp() && data[0] != null && Arrays.equals(data[0], URL_QUERY_METRICS_FIRST_PARAM_BYTES)) {
            var sw = new StringWriter();
            try {
                TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples());
                return new BulkReply(sw.toString().getBytes());
            } catch (IOException e) {
                return new ErrorReply(e.getMessage());
            }
        }

        if (data[0] == null) {
            return ErrorReply.FORMAT;
        }

        var cmd = request.cmd();
        if (cmd.equals(PING_COMMAND)) {
            return PongReply.INSTANCE;
        }

        var doLogCmd = Debug.getInstance().logCmd;
        if (doLogCmd) {
            if (data.length == 1) {
                log.info("Request cmd: {}", cmd);
            } else {
                var sb = new StringBuilder();
                sb.append("Request cmd: ").append(cmd).append(" ");
                for (int i = 1; i < data.length; i++) {
                    sb.append(new String(data[i])).append(" ");
                }
                log.info(sb.toString());
            }
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
            AfterAuthFlagHolder.add(remoteAddress);
            return OKReply.INSTANCE;
        }

        if (password != null && !AfterAuthFlagHolder.contains(remoteAddress)) {
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

            // for slave can connect to master, check values
            var key = new String(keyBytes);
            if (key.equals(XGroup.CONF_FOR_SLOT_KEY)) {
                var map = ConfForSlot.global.slaveCanMatchCheckValues();
                var objectMapper = new ObjectMapper();
                try {
                    var jsonStr = objectMapper.writeValueAsString(map);
                    return new BulkReply(jsonStr.getBytes());
                } catch (JsonProcessingException e) {
                    return new ErrorReply(e.getMessage());
                }
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
            } catch (Exception e) {
                log.error("Get error, key: " + new String(keyBytes), e);
                return new ErrorReply(e.getMessage());
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
            } catch (Exception e) {
                log.error("Set error, key: " + new String(keyBytes), e);
                return new ErrorReply(e.getMessage());
            }

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
        } catch (Exception e) {
            log.error("Request handle error", e);
            return new ErrorReply(e.getMessage());
        }

        return ErrorReply.FORMAT;
    }

    final static SimpleGauge sampleToTrainSizeGauge = new SimpleGauge("sample_to_train_size", "sample to train size",
            "worker_id");

    static {
        sampleToTrainSizeGauge.register();
    }

    private void initMetricsCollect() {
        sampleToTrainSizeGauge.addRawGetter(() -> {
            var labelValues = List.of(workerIdStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("sample_to_train_size", new SimpleGauge.ValueWithLabelValues((double) sampleToTrainList.size(), labelValues));
            return map;
        });
    }
}
