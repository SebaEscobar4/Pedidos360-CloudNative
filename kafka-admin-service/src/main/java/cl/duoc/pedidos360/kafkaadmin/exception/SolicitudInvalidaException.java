package cl.duoc.pedidos360.kafkaadmin.exception;

/**
 * 400 Bad Request: parametros o configuraciones que Kafka no acepta.
 */
public class SolicitudInvalidaException extends RuntimeException {

    public SolicitudInvalidaException(String message) {
        super(message);
    }

    public SolicitudInvalidaException(String message, Throwable cause) {
        super(message, cause);
    }
}
