package redis.persist;

public class BucketFullException extends RuntimeException {
    public BucketFullException(String message) {
        super(message);
    }
}
