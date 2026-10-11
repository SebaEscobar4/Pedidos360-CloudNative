package cl.duoc.pedidos360.notificaciones.messaging;

/**
 * Error transitorio (disco ocupado, servicio externo caido) que puede resolverse solo.
 * El consumidor reintenta el mensaje hasta el maximo configurado y recien entonces lo envia a la DLQ.
 */
public class ErrorRecuperableException extends RuntimeException {

    public ErrorRecuperableException(String message, Throwable cause) {
        super(message, cause);
    }
}
