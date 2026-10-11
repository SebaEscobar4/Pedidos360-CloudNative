package cl.duoc.pedidos360.notificaciones.messaging;

/**
 * Error que NO se soluciona reintentando (mensaje mal formado, datos invalidos).
 * El consumidor responde con NACK sin requeue y el mensaje va directo a la DLQ.
 */
public class ErrorNoRecuperableException extends RuntimeException {

    public ErrorNoRecuperableException(String message) {
        super(message);
    }

    public ErrorNoRecuperableException(String message, Throwable cause) {
        super(message, cause);
    }
}
