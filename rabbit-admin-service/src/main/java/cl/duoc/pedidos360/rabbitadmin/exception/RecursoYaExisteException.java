package cl.duoc.pedidos360.rabbitadmin.exception;

/** Ya existe una cola o exchange con ese nombre (HTTP 409). */
public class RecursoYaExisteException extends RuntimeException {

    public RecursoYaExisteException(String message) {
        super(message);
    }
}
