package cl.duoc.pedidos360.notificaciones.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.pedidos360.notificaciones.config.RabbitMQProperties;

/**
 * =========================================================================
 * MENSAJERIA: ManejadorErroresMensajeria
 * =========================================================================
 * FUNCION: Concentra las dos decisiones posibles cuando un consumidor falla:
 *
 *   enviarADlq()  -> NACK sin requeue. RabbitMQ mueve el mensaje al Dead Letter Exchange
 *                    y de ahi a la DLQ de la cola. Se usa para errores NO recuperables.
 *
 *   reintentar()  -> Para errores recuperables. Vuelve a publicar el mensaje en SU MISMA cola
 *                    con el contador 'x-reintentos' incrementado y confirma (ACK) el original.
 *                    Al superar el maximo configurado, el mensaje se envia a la DLQ.
 *                    Asi nunca hay requeue infinito ni perdida de mensajes.
 *
 * CONECTA CON: Los consumidores @RabbitListener de 'notificacion' y 'ticket'.
 */
@Component
public class ManejadorErroresMensajeria {

    private static final Logger log = LoggerFactory.getLogger(ManejadorErroresMensajeria.class);

    // Exchange por defecto de RabbitMQ: enruta directo a la cola cuyo nombre es la routing key
    private static final String EXCHANGE_POR_DEFECTO = "";

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQProperties props;

    public ManejadorErroresMensajeria(RabbitTemplate rabbitTemplate, RabbitMQProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    public void enviarADlq(Message message, Channel channel, Exception causa) throws IOException {
        log.error("[DLQ] Mensaje rechazado en cola '{}' y enviado a DLQ. motivo={} cuerpo={}",
                message.getMessageProperties().getConsumerQueue(),
                causa.getMessage(),
                new String(message.getBody()));
        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
    }

    public void reintentar(Message message, Channel channel, Exception causa) throws IOException {
        int reintentosPrevios = leerReintentos(message);
        if (reintentosPrevios >= props.reintentos().maximo()) {
            log.error("[REINTENTO] Se agotaron los {} reintentos.", props.reintentos().maximo());
            enviarADlq(message, channel, causa);
            return;
        }

        String cola = message.getMessageProperties().getConsumerQueue();
        Message copia = MessageBuilder.fromMessage(message)
                .setHeader(props.reintentos().header(), reintentosPrevios + 1)
                .build();
        rabbitTemplate.send(EXCHANGE_POR_DEFECTO, cola, copia);
        log.warn("[REINTENTO] Intento {}/{} en cola '{}'. motivo={}",
                reintentosPrevios + 1, props.reintentos().maximo(), cola, causa.getMessage());
        // El original se confirma solo despues de dejar encolada la copia para el reintento
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    private int leerReintentos(Message message) {
        Object valor = message.getMessageProperties().getHeaders().get(props.reintentos().header());
        return valor instanceof Number numero ? numero.intValue() : 0;
    }
}
