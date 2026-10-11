package cl.duoc.pedidos360.rabbitadmin.exception;

/** El nombre recibido en la URL no es un nombre de recurso valido (HTTP 400). */
public class NombreInvalidoException extends RuntimeException {

    public NombreInvalidoException(String message) {
        super(message);
    }
}
