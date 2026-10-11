package cl.duoc.pedidos360.rabbitadmin.exception;

/** La cola o el exchange indicado no existe en el broker (HTTP 404). */
public class RecursoNoEncontradoException extends RuntimeException {

    public RecursoNoEncontradoException(String message) {
        super(message);
    }
}
