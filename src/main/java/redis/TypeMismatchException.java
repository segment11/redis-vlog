package redis;

public class TypeMismatchException extends RuntimeException {
    public TypeMismatchException(String message) {
        super(message);
    }
}
