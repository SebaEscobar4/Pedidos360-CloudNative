package cl.duoc.pedidos360.notificaciones.messaging;

import java.io.IOException;

import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * =========================================================================
 * MENSAJERIA: LectorEventos
 * =========================================================================
 * FUNCION: Convierte el cuerpo JSON de un mensaje AMQP en un PedidoEvento.
 *          Un cuerpo ilegible es un error NO recuperable: reintentar no lo arregla.
 * CONECTA CON: Los consumidores @RabbitListener.
 */
@Component
public class LectorEventos {

    private final ObjectMapper objectMapper;

    public LectorEventos(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public PedidoEvento leerPedidoEvento(Message message) {
        try {
            PedidoEvento evento = objectMapper.readValue(message.getBody(), PedidoEvento.class);
            if (evento == null || evento.numeroPedido() == null || evento.numeroPedido().isBlank()) {
                throw new ErrorNoRecuperableException("El evento no trae numero de pedido");
            }
            return evento;
        } catch (IOException e) {
            throw new ErrorNoRecuperableException("El cuerpo del mensaje no es un PedidoEvento JSON valido", e);
        }
    }
}
