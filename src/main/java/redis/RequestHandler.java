package redis;

import com.github.luben.zstd.Zstd;
import io.activej.config.Config;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.common.TextFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.command.*;
import redis.decode.Request;
import redis.metric.SimpleGauge;
import redis.persist.ReadonlyException;
import redis.repl.LeaderSelector;
import redis.reply.*;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.activej.config.converter.ConfigConverters.ofBoolean;
import static io.activej.config.converter.ConfigConverters.ofInteger;

public class RequestHandler {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final String PING_COMMAND = "ping";
    private static final String AUTH_COMMAND = "auth";
    private static final String GET_COMMAND = "get";
    private static final String SET_COMMAND = "set";
    private static final String QUIT_COMMAND = "quit";
    private static final String ERROR_FOR_STAT_AS_COMMAND = "x_error";
    private static final String READONLY_FOR_STAT_AS_COMMAND = "x_readonly";

    @VisibleForTesting
    final byte workerId;
    private final String workerIdStr;
    @VisibleForTesting
    final byte netWorkers;
    @VisibleForTesting
    final short slotNumber;
    @VisibleForTesting
    final SnowFlake snowFlake;

    @VisibleForTesting
    String password;

    @TestOnly
    final boolean localTest;
    @TestOnly
    final int localTestRandomValueListSize;
    @TestOnly
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

        this.password = ConfForGlobal.PASSWORD;

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

        this.compressStats = new CompressStats("net_worker_" + workerId, "net_");
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
            if (GET_COMMAND.equals(cmd)) {
                if (data.length >= 2) {
                    var keyBytes = data[1];
                    var key = new String(keyBytes);
                    if (key.startsWith(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH)) {
                        var dataTransfer = transferDataForXGroup(key);
                        var slotWithKeyHashListForXGroup = XGroup.parseSlots(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH, dataTransfer, request.getSlotNumber());
                        request.setSlotWithKeyHashList(slotWithKeyHashListForXGroup);
                        return;
                    }
                }
            }

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

    private static final byte[] URL_QUERY_METRICS_BYTES = "metrics".getBytes();
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_MASTER = "master";
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_MASTER_OR_SLAVE = "master_or_slave";
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_SLAVE = "slave";
    private static final String URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE = "slave_with_zone";
    private static final String URL_QUERY_FOR_CMD_STAT_COUNT = "cmd_stat_count";
    private static final String HEADER_NAME_FOR_BASIC_AUTH = "Authorization";

    private static byte[][] transferDataForXGroup(String keyAsData) {
        // eg. get x_repl,sub_cmd,sub_sub_cmd,***
        // transfer data to: x_repl sub_cmd sub_sub_cmd ***
        var array = keyAsData.split(",");
        var dataTransfer = new byte[array.length][];
        for (int i = 0; i < array.length; i++) {
            dataTransfer[i] = array[i].getBytes();
        }
        return dataTransfer;
    }

    // redis cmd is less than 1k, each group eg: AGroup is less than 200
    private String[][] cmdStatArray = new String[26][200];
    private long[][] cmdStatCountArray = new long[26][200];

    @VisibleForTesting
    int increaseCmdStatArray(byte firstByte, String cmd) {
        var index = firstByte - 'a';
        var stringArray = cmdStatArray[index];
        var countArray = cmdStatCountArray[index];
        for (int i = 0; i < stringArray.length; i++) {
            if (stringArray[i] == null) {
                stringArray[i] = cmd;
                countArray[i] = 1;
                return i;
            }

            if (stringArray[i].equals(cmd)) {
                countArray[i]++;
                return i;
            }
        }

        return -1;
    }

    @VisibleForTesting
    String cmdStatAsPrometheusFormatString() {
        var sb = new StringBuilder();
        for (int i = 0; i < cmdStatArray.length; i++) {
            var stringArray = cmdStatArray[i];
            var countArray = cmdStatCountArray[i];
            for (int j = 0; j < stringArray.length; j++) {
                if (stringArray[j] == null) {
                    break;
                }

                sb.append("cmd_stat_count{cmd=\"").append(stringArray[j]).append("\",worker_id=\"").append(workerIdStr).append("\"} ").append(countArray[j]).append("\n");
            }
        }

        return sb.toString();
    }

    @TestOnly
    long cmdStatCountTotal() {
        long total = 0;
        for (int i = 0; i < cmdStatCountArray.length; i++) {
            var countArray = cmdStatCountArray[i];
            for (long count : countArray) {
                total += count;
            }
        }

        return total;
    }

    private long getCmdCountStat(String cmd) {
        var firstByte = cmd.charAt(0);
        var index = firstByte - 'a';
        var stringArray = cmdStatArray[index];
        var countArray = cmdStatCountArray[index];

        for (int i = 0; i < stringArray.length; i++) {
            if (stringArray[i] == null) {
                break;
            }

            if (stringArray[i].equals(cmd)) {
                return countArray[i];
            }
        }

        return 0;
    }

    private static Summary requestTimeSummary = Summary.build()
            .name("request_time")
            .help("Request time in seconds.")
            .labelNames("command")
            .maxAgeSeconds(60)
            .ageBuckets(5)
            .quantile(0.90, 0.05)
            .quantile(0.999, 0.001)
            .register();

    Reply handle(@NotNull Request request, ITcpSocket socket) {
        if (isStopped) {
            return ErrorReply.SERVER_STOPPED;
        }

        var data = request.getData();

        if (request.isRepl()) {
            var xGroup = new XGroup(null, data, socket);
            xGroup.init(this, request);

            // try catch in handle repl method
            return xGroup.handleRepl();
        }

        // http special handle
        if (request.isHttp() && data.length == 1) {
            // metrics, prometheus format
            // url should be ?metrics, eg: http://localhost:7379/?metrics
            // for one target slot beside 0 metrics: http://localhost:7379/?manage&slot&0&view-metrics
            var firstDataBytes = data[0];
            if (Arrays.equals(firstDataBytes, URL_QUERY_METRICS_BYTES)) {
                var sw = new StringWriter();
                try {
                    TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples());
                    return new BulkReply(sw.toString().getBytes());
                } catch (IOException e) {
                    return new ErrorReply(e.getMessage());
                }
            }

            // for haproxy
            if (firstDataBytes == null) {
                return ErrorReply.FORMAT;
            }

            var firstDataString = new String(firstDataBytes);
            if (firstDataString.equals(URL_QUERY_FOR_HAPROXY_FILTER_MASTER)) {
                var isMaster = LeaderSelector.getInstance().hasLeadership();
                if (isMaster) {
                    // will response 200 status code
                    return new BulkReply("master".getBytes());
                } else {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }
            }

            if (firstDataString.equals(URL_QUERY_FOR_HAPROXY_FILTER_MASTER_OR_SLAVE)) {
                // will response 200 status code
                return new BulkReply("master or slave".getBytes());
            }

            if (firstDataString.equals(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE)) {
                var isMaster = LeaderSelector.getInstance().hasLeadership();
                if (!isMaster) {
                    // will response 200 status code
                    return new BulkReply("slave".getBytes());
                } else {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }
            }

            if (firstDataString.startsWith(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE)) {
                // eg. slave_with_zone=zone1
                var targetZone = firstDataString.substring(URL_QUERY_FOR_HAPROXY_FILTER_SLAVE_WITH_ZONE.length() + 1);
                var isMaster = LeaderSelector.getInstance().hasLeadership();
                if (isMaster) {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }

                if (targetZone.equals(ConfForGlobal.targetAvailableZone)) {
                    // will response 200 status code
                    return new BulkReply(("slave with zone " + targetZone).getBytes());
                } else {
                    // will response 404 status code
                    return NilReply.INSTANCE;
                }
            }

            // cmd_stat_count or cmd_stat_count=cmd or cmd_stat_count=all
            if (firstDataString.startsWith(URL_QUERY_FOR_CMD_STAT_COUNT)) {
                var cmd = firstDataString.length() <= (URL_QUERY_FOR_CMD_STAT_COUNT.length() + 1) ? "all" :
                        firstDataString.substring(URL_QUERY_FOR_CMD_STAT_COUNT.length() + 1);
                if ("all".equals(cmd)) {
                    return new BulkReply(cmdStatAsPrometheusFormatString().getBytes());
                } else {
                    return new BulkReply(String.valueOf(getCmdCountStat(cmd)).getBytes());
                }
            }
        }

        if (data[0] == null) {
            return ErrorReply.FORMAT;
        }

        var cmd = request.cmd();
        var requestTimer = requestTimeSummary.labels(cmd).startTimer();
        try {
            if (cmd.equals(PING_COMMAND)) {
                increaseCmdStatArray((byte) 'p', PING_COMMAND);

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
            // http basic auth
            if (request.isHttp()) {
                if (!AfterAuthFlagHolder.contains(remoteAddress) && password != null) {
                    var headerValue = request.getHttpHeader(HEADER_NAME_FOR_BASIC_AUTH);
                    if (headerValue == null) {
                        return ErrorReply.NO_AUTH;
                    }

                    // base64 decode
                    // trim "Basic " prefix
                    var auth = new String(Base64.getDecoder().decode(headerValue.substring(6)));
                    // skip username
                    if (!password.equals(auth.substring(auth.indexOf(':') + 1))) {
                        return ErrorReply.AUTH_FAILED;
                    }

                    AfterAuthFlagHolder.add(remoteAddress);
                    // continue to handle request
                }
            } else {
                if (cmd.equals(AUTH_COMMAND)) {
                    increaseCmdStatArray((byte) 'a', AUTH_COMMAND);

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
            }

            if (password != null && !AfterAuthFlagHolder.contains(remoteAddress)) {
                return ErrorReply.NO_AUTH;
            }

            if (cmd.equals(GET_COMMAND)) {
                increaseCmdStatArray((byte) 'g', GET_COMMAND);

                if (data.length != 2) {
                    return ErrorReply.FORMAT;
                }

                var keyBytes = data[1];
                if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                    return ErrorReply.KEY_TOO_LONG;
                }

                var key = new String(keyBytes);
                if (key.startsWith(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH)) {
                    // dispatch to XGroup
                    // eg. get x_repl,sub_cmd,sub_sub_cmd,***
                    var dataTransfer = transferDataForXGroup(key);
                    // transfer data to: x_repl sub_cmd sub_sub_cmd ***
                    var xGroup = new XGroup(XGroup.X_REPL_AS_GET_CMD_KEY_PREFIX_FOR_DISPATCH, dataTransfer, socket).init(this, request);
                    try {
                        return xGroup.handle();
                    } catch (Exception e) {
                        increaseCmdStatArray((byte) 'e', ERROR_FOR_STAT_AS_COMMAND);
                        log.error("XGroup handle error", e);
                        return new ErrorReply(e.getMessage());
                    }
                }

                var gGroup = new GGroup(cmd, data, socket).init(this, request);
                try {
                    var slotWithKeyHashList = request.getSlotWithKeyHashList();
                    var bytes = gGroup.get(keyBytes, slotWithKeyHashList.get(0), true);
                    return bytes != null ? new BulkReply(bytes) : NilReply.INSTANCE;
                } catch (TypeMismatchException e) {
                    increaseCmdStatArray((byte) 'e', ERROR_FOR_STAT_AS_COMMAND);
                    return new ErrorReply(e.getMessage());
                } catch (DictMissingException e) {
                    return ErrorReply.DICT_MISSING;
                } catch (Exception e) {
                    increaseCmdStatArray((byte) 'e', ERROR_FOR_STAT_AS_COMMAND);
                    log.error("Get error, key: " + new String(keyBytes), e);
                    return new ErrorReply(e.getMessage());
                }
            }

            // for short
            // full set command handle in SGroup
            if (cmd.equals(SET_COMMAND) && data.length == 3) {
                increaseCmdStatArray((byte) 's', SET_COMMAND);

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
                    increaseCmdStatArray((byte) 'r', READONLY_FOR_STAT_AS_COMMAND);
                    return ErrorReply.READONLY;
                } catch (Exception e) {
                    increaseCmdStatArray((byte) 'e', ERROR_FOR_STAT_AS_COMMAND);
                    log.error("Set error, key: " + new String(keyBytes), e);
                    return new ErrorReply(e.getMessage());
                }

                return OKReply.INSTANCE;
            }

            // else, use enum better
            var firstByte = data[0][0];
            try {
                if (firstByte == 'a' || firstByte == 'A') {
                    increaseCmdStatArray((byte) 'a', cmd);
                    return new AGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'b' || firstByte == 'B') {
                    increaseCmdStatArray((byte) 'b', cmd);
                    return new BGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'c' || firstByte == 'C') {
                    increaseCmdStatArray((byte) 'c', cmd);
                    return new CGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'd' || firstByte == 'D') {
                    increaseCmdStatArray((byte) 'd', cmd);
                    return new DGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'e' || firstByte == 'E') {
                    increaseCmdStatArray((byte) 'e', cmd);
                    return new EGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'f' || firstByte == 'F') {
                    increaseCmdStatArray((byte) 'f', cmd);
                    return new FGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'g' || firstByte == 'G') {
                    increaseCmdStatArray((byte) 'g', cmd);
                    return new GGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'h' || firstByte == 'H') {
                    increaseCmdStatArray((byte) 'h', cmd);
                    return new HGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'i' || firstByte == 'I') {
                    increaseCmdStatArray((byte) 'i', cmd);
                    return new IGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'j' || firstByte == 'J') {
                    increaseCmdStatArray((byte) 'j', cmd);
                    return new JGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'k' || firstByte == 'K') {
                    increaseCmdStatArray((byte) 'k', cmd);
                    return new KGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'l' || firstByte == 'L') {
                    increaseCmdStatArray((byte) 'l', cmd);
                    return new LGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'm' || firstByte == 'M') {
                    increaseCmdStatArray((byte) 'm', cmd);
                    return new MGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'n' || firstByte == 'N') {
                    increaseCmdStatArray((byte) 'n', cmd);
                    return new NGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'o' || firstByte == 'O') {
                    increaseCmdStatArray((byte) 'o', cmd);
                    return new OGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'p' || firstByte == 'P') {
                    increaseCmdStatArray((byte) 'p', cmd);
                    return new PGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'q' || firstByte == 'Q') {
                    increaseCmdStatArray((byte) 'q', cmd);
                    return new QGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'r' || firstByte == 'R') {
                    increaseCmdStatArray((byte) 'r', cmd);
                    return new RGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 's' || firstByte == 'S') {
                    increaseCmdStatArray((byte) 's', cmd);
                    return new SGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 't' || firstByte == 'T') {
                    increaseCmdStatArray((byte) 't', cmd);
                    return new TGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'u' || firstByte == 'U') {
                    increaseCmdStatArray((byte) 'u', cmd);
                    return new UGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'v' || firstByte == 'V') {
                    increaseCmdStatArray((byte) 'v', cmd);
                    return new VGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'w' || firstByte == 'W') {
                    increaseCmdStatArray((byte) 'w', cmd);
                    return new WGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'x' || firstByte == 'X') {
                    increaseCmdStatArray((byte) 'x', cmd);
                    return new XGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'y' || firstByte == 'Y') {
                    increaseCmdStatArray((byte) 'y', cmd);
                    return new YGroup(cmd, data, socket).init(this, request).handle();
                } else if (firstByte == 'z' || firstByte == 'Z') {
                    increaseCmdStatArray((byte) 'z', cmd);
                    return new ZGroup(cmd, data, socket).init(this, request).handle();
                }
            } catch (ReadonlyException e) {
                increaseCmdStatArray((byte) 'r', READONLY_FOR_STAT_AS_COMMAND);
                return ErrorReply.READONLY;
            } catch (Exception e) {
                increaseCmdStatArray((byte) 'e', ERROR_FOR_STAT_AS_COMMAND);
                log.error("Request handle error", e);
                return new ErrorReply(e.getMessage());
            }

            return ErrorReply.FORMAT;
        } finally {
            requestTimer.observeDuration();
        }
    }

    @VisibleForTesting
    final static SimpleGauge requestHandlerGauge = new SimpleGauge("request_handler", "Net worker request handler metrics.",
            "worker_id");

    static {
        requestHandlerGauge.register();
    }

    private void initMetricsCollect() {
        requestHandlerGauge.addRawGetter(() -> {
            var labelValues = List.of(workerIdStr);

            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();
            map.put("request_sample_to_train_size", new SimpleGauge.ValueWithLabelValues((double) sampleToTrainList.size(), labelValues));
            return map;
        });
    }
}
