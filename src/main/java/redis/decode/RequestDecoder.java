package redis.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.repl.Repl;

import java.util.ArrayList;
import java.util.Arrays;

import static redis.repl.Repl.PROTOCOL_KEYWORD_BYTES;

public class RequestDecoder implements ByteBufsDecoder<ArrayList<Request>> {
    // in local thread
    private final RESP resp = new RESP();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private Request tryDecodeOne(ByteBufs bufs) {
        io.netty.buffer.CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        int capacity = 0;
        for (var buf : bufs) {
            int remainingN = buf.readRemaining();
            if (remainingN > 0) {
                capacity += remainingN;
                compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(buf.array(), buf.head(), remainingN));
            }
        }
        if (capacity == 0) {
            return null;
        }

        compositeByteBuf.readerIndex(0);
        compositeByteBuf.writerIndex(capacity).capacity(capacity);

        // http or redis
        // first 6 bytes
        var first6 = new byte[6];
        compositeByteBuf.readBytes(first6);
        var isGet = Arrays.equals(first6, 0, 3, HttpHeaderBody.GET, 0, 3);
        var isPost = Arrays.equals(first6, 0, 4, HttpHeaderBody.POST, 0, 4);
        var isPut = Arrays.equals(first6, 0, 3, HttpHeaderBody.PUT, 0, 3);
        var isDelete = Arrays.equals(first6, 0, 6, HttpHeaderBody.DELETE, 0, 6);
        boolean isHttp = isGet || isPost || isPut || isDelete;

        var isRepl = Arrays.equals(first6, 0, 6, PROTOCOL_KEYWORD_BYTES, 0, 6);

        // set reader index back
        compositeByteBuf.readerIndex(0);

        byte[][] data;
        if (isHttp) {
            var h = new HttpHeaderBody();
            h.feed(compositeByteBuf, compositeByteBuf.readableBytes(), 0);
            if (!h.isOk) {
                return null;
//                  throw new MalformedDataException("Malformed data");
            }

            if (isGet || isDelete) {
                // query parameters
                var pos = h.url.indexOf("?");
                if (pos == -1) {
                    return null;
                }

                var arr = h.url.substring(pos + 1).split("&");
                data = new byte[arr.length][];
                for (int i = 0; i < arr.length; i++) {
                    data[i] = arr[i].getBytes();
                }
            } else {
                var body = h.body();
                if (body == null) {
                    return null;
                }

                var arr = new String(body).split(" ");
                data = new byte[arr.length][];
                for (int i = 0; i < arr.length; i++) {
                    data[i] = arr[i].getBytes();
                }
            }
        } else if (isRepl) {
            data = Repl.decode(compositeByteBuf);
        } else {
            data = resp.decode(compositeByteBuf);
            if (data == null) {
                return null;
//                throw new MalformedDataException("Malformed data");
            }
        }

        boolean isFinished = true;
        for (var inner : data) {
            if (inner == null) {
                isFinished = false;
                break;
            }
        }
        if (!isFinished) {
            return null;
        }

        // remove already consumed bytes
        int consumedN = compositeByteBuf.readerIndex();
        bufs.takeExactSize(consumedN);

        return new Request(data, isHttp, isRepl);
    }

    @Override
    public @Nullable ArrayList<Request> tryDecode(ByteBufs bufs) throws MalformedDataException {
        try {
            ArrayList<Request> pipeline = new ArrayList<>();
            var one = tryDecodeOne(bufs);
            while (one != null) {
                pipeline.add(one);
                one = tryDecodeOne(bufs);
            }
            return pipeline.isEmpty() ? null : pipeline;
        } catch (Exception e) {
            log.error("Decode error", e);
            return null;
//            throw e;
        }
    }
}
