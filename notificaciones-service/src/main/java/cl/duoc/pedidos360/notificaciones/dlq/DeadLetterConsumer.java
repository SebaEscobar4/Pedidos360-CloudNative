package cl.duoc.pedidos360.notificaciones.dlq;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

/**
 * =========================================================================
 * CONSUMIDOR: DeadLetterConsumer (dominio 'dlq')
 * =========================================================================
 * FUNCION: Escucha las Dead Letter Queues y deja registro en el log (nivel ERROR) de cada
 *          mensaje que no pudo procesarse: cola de origen, motivo y contenido.
 *          Se puede apagar con app.rabbitmq.dlq.listener-habilitado=false para que los
 *          mensajes queden guardados en la DLQ y se revisen en la consola de RabbitMQ.
 * CONECTA CON: Las colas *.dlq declaradas en RabbitMQConfig.
 */
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    @RabbitListener(
            queues = {
                "${app.rabbitmq.queues.notificaciones-dlq}",
                "${app.rabbitmq.queues.tickets-dlq}"
            },
            autoStartup = "${app.rabbitmq.dlq.listener-habilitado}")
    public void onMensajeMuerto(Message message, Channel channel) throws IOException {
        log.error("[DLQ] Mensaje muerto recibido. dlq={} colaOrigen={} motivo={} cuerpo={}",
                message.getMessageProperties().getConsumerQueue(),
                datoXDeath(message, "queue"),
                datoXDeath(message, "reason"),
                new String(message.getBody()));
        // Ya quedo registrado: se confirma para que no vuelva a entregarse
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    // RabbitMQ agrega el header 'x-death' con el historial de por que murio el mensaje
    private Object datoXDeath(Message message, String clave) {
        Object xDeath = message.getMessageProperties().getHeaders().get("x-death");
        if (xDeath instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof Map<?, ?> primero) {
            return primero.get(clave);
        }
        return "desconocido";
    }
}
