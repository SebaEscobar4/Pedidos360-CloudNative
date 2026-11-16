package cl.duoc.pedidos360.kafkaadmin.exception;

/**
 * 503 Service Unavailable: el cluster no respondio a tiempo.
 */
public class KafkaNoDisponibleException extends RuntimeException {

    public KafkaNoDisponibleException(String message) {
        super(message);
    }

    public KafkaNoDisponibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
