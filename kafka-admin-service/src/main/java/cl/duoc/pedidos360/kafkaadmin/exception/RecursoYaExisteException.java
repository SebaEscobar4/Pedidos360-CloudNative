package cl.duoc.pedidos360.kafkaadmin.exception;

/**
 * 409 Conflict: el topico ya existe.
 */
public class RecursoYaExisteException extends RuntimeException {

    public RecursoYaExisteException(String message) {
        super(message);
    }

    public RecursoYaExisteException(String message, Throwable cause) {
        super(message, cause);
    }
}
