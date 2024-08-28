package redis;

public class DictMissingException extends RuntimeException {
    public DictMissingException(String message) {
        super(message);
    }
}
