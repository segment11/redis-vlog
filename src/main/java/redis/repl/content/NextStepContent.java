package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class NextStepContent implements ReplContent {
    public static final NextStepContent INSTANCE = new NextStepContent();

    private NextStepContent() {
    }

    public static boolean isNextStep(byte[] contentBytes) {
        return contentBytes.length == 1 && contentBytes[0] == 0;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte((byte) 0);
    }

    @Override
    public int encodeLength() {
        return 1;
    }
}
