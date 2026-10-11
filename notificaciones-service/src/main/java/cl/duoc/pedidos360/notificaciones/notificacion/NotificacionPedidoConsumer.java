package cl.duoc.pedidos360.notificaciones.notificacion;

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
 * CONSUMIDOR: NotificacionPedidoConsumer (dominio 'notificacion')
 * =========================================================================
 * FUNCION: Escucha la cola de notificaciones (pedido creado + cambios de estado).
 *          Confirmacion MANUAL de cada mensaje:
 *            - Exito                 -> ACK.
 *            - Error no recuperable  -> NACK sin requeue -> DLQ.
 *            - Error recuperable     -> reintento controlado; al agotarse -> DLQ.
 * CONECTA CON: NotificacionService (negocio) y ManejadorErroresMensajeria (reintentos/DLQ).
 */
@Component
public class NotificacionPedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificacionPedidoConsumer.class);

    private final NotificacionService notificacionService;
    private final LectorEventos lectorEventos;
    private final ManejadorErroresMensajeria manejadorErrores;

    public NotificacionPedidoConsumer(
            NotificacionService notificacionService,
            LectorEventos lectorEventos,
            ManejadorErroresMensajeria manejadorErrores) {
        this.notificacionService = notificacionService;
        this.lectorEventos = lectorEventos;
        this.manejadorErrores = manejadorErrores;
    }

    @RabbitListener(queues = "${app.rabbitmq.queues.notificaciones}")
    public void onEventoPedido(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            PedidoEvento evento = lectorEventos.leerPedidoEvento(message);
            notificacionService.notificar(evento);
            channel.basicAck(deliveryTag, false);
            log.debug("[NOTIFICACION] ACK pedido {}", evento.numeroPedido());
        } catch (ErrorNoRecuperableException e) {
            manejadorErrores.enviarADlq(message, channel, e);
        } catch (Exception e) {
            // Cualquier otro fallo se considera transitorio
            manejadorErrores.reintentar(message, channel, e);
        }
    }
}
