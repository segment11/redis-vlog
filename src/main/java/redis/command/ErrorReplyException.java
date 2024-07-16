package redis.command;

public class ErrorReplyException extends RuntimeException {
    public ErrorReplyException(String message) {
        super(message);
    }
}
