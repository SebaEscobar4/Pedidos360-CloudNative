package cl.duoc.pedidos360.kafkaadmin.exception;

/**
 * 404 Not Found: el topico o consumer group no existe.
 */
public class RecursoNoEncontradoException extends RuntimeException {

    public RecursoNoEncontradoException(String message) {
        super(message);
    }

    public RecursoNoEncontradoException(String message, Throwable cause) {
        super(message, cause);
    }
}
