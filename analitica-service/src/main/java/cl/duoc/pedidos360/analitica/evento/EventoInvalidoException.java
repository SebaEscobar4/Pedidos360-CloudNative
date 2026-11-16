package cl.duoc.pedidos360.analitica.evento;

/**
 * Error NO recuperable: el evento no cumple el contrato esperado. Reintentarlo no lo arregla,
 * por lo que el manejador de errores lo envia directo al Dead Letter Topic.
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
