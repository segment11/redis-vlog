package redis.persist;

public class SegmentOverflowException extends RuntimeException {
    SegmentOverflowException(String message) {
        super(message);
    }
}
