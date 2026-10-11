package cl.duoc.pedidos360.notificaciones.ticket;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.pedidos360.notificaciones.messaging.ErrorNoRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.LectorEventos;
import cl.duoc.pedidos360.notificaciones.messaging.ManejadorErroresMensajeria;
import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * =========================================================================
 * CONSUMIDOR: TicketPedidoConsumer (dominio 'ticket')
 * =========================================================================
 * FUNCION: Escucha la cola de tickets (solo pedidos creados) y genera el ticket de compra.
 *          Confirmacion MANUAL de cada mensaje:
 *            - Exito                 -> ACK.
 *            - Error no recuperable  -> NACK sin requeue -> DLQ.
 *            - Error recuperable     -> reintento controlado; al agotarse -> DLQ.
 * CONECTA CON: TicketService (negocio) y ManejadorErroresMensajeria (reintentos/DLQ).
 */
@Component
public class TicketPedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(TicketPedidoConsumer.class);

    private final TicketService ticketService;
    private final LectorEventos lectorEventos;
    private final ManejadorErroresMensajeria manejadorErrores;

    public TicketPedidoConsumer(
            TicketService ticketService,
            LectorEventos lectorEventos,
            ManejadorErroresMensajeria manejadorErrores) {
        this.ticketService = ticketService;
        this.lectorEventos = lectorEventos;
        this.manejadorErrores = manejadorErrores;
    }

    @RabbitListener(queues = "${app.rabbitmq.queues.tickets}")
    public void onPedidoCreado(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            PedidoEvento evento = lectorEventos.leerPedidoEvento(message);
            ticketService.generar(evento);
            channel.basicAck(deliveryTag, false);
            log.debug("[TICKET] ACK pedido {}", evento.numeroPedido());
        } catch (ErrorNoRecuperableException e) {
            manejadorErrores.enviarADlq(message, channel, e);
        } catch (Exception e) {
            // ErrorRecuperableException y cualquier otro fallo inesperado
            manejadorErrores.reintentar(message, channel, e);
        }
    }
}
