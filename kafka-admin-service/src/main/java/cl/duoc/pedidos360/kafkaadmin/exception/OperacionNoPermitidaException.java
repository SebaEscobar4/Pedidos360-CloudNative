package cl.duoc.pedidos360.kafkaadmin.exception;

/**
 * 409 Conflict: la operacion choca con el estado actual (topico protegido, grupo con consumidores activos).
 */
public class OperacionNoPermitidaException extends RuntimeException {

    public OperacionNoPermitidaException(String message) {
        super(message);
    }

    public OperacionNoPermitidaException(String message, Throwable cause) {
        super(message, cause);
    }
}
