package redis.persist;

public class SegmentOverflowException extends RuntimeException {
    public SegmentOverflowException(String message) {
        super(message);
    }
}
