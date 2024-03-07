package redis.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class RequestDecoder implements ByteBufsDecoder<Request> {
    // in local thread
    private final RESP resp = new RESP();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public @Nullable Request tryDecode(ByteBufs bufs) throws MalformedDataException {
        io.netty.buffer.CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        int capacity = 0;
        for (var buf : bufs) {
            if (buf.readRemaining() > 0) {
                capacity += buf.tail();
                compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(buf.array()));
            }
        }

        int head = bufs.peekBuf().head();
        compositeByteBuf.readerIndex(head);
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

        // set reader index back
        compositeByteBuf.readerIndex(head);

        try {
            byte[][] data;
            if (isHttp) {
                var h = new HttpHeaderBody();
                h.feed(compositeByteBuf, compositeByteBuf.readableBytes(), head);
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
            int consumedN = compositeByteBuf.readerIndex() - head;
            bufs.takeExactSize(consumedN);

            return new Request(data, isHttp);
//        } catch (IllegalArgumentException e) {
//            throw new MalformedDataException("Malformed data: " + e.getMessage());
        } catch (Exception e) {
            log.error("Decode error", e);
            return null;
//            throw e;
        }
    }
}
