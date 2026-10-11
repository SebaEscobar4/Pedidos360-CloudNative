package cl.duoc.pedidos360.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import cl.duoc.pedidos360.config.RabbitMQProperties;

/**
 * =========================================================================
 * PRODUCTOR: PedidoEventPublisher
 * =========================================================================
 * FUNCION: Publica en RabbitMQ los eventos de pedidos emitidos por PedidoService.
 *          - Se ejecuta DESPUES del commit: nunca se avisa de un pedido que no quedo guardado.
 *          - Si el broker no esta disponible, se registra el error y el pedido sigue su
 *            curso normal: la mensajeria no afecta la logica existente del sistema.
 * CONECTA CON: PedidoService (via eventos de Spring) y RabbitMQConfig (RabbitTemplate).
 */
@Component
public class PedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQProperties props;

    public PedidoEventPublisher(RabbitTemplate rabbitTemplate, RabbitMQProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publicar(PedidoEvento evento) {
        String routingKey = switch (evento.tipo()) {
            case PEDIDO_CREADO -> props.routingKeys().pedidoCreado();
            case ESTADO_ACTUALIZADO -> props.routingKeys().pedidoEstadoActualizado();
        };
        try {
            rabbitTemplate.convertAndSend(props.exchanges().pedidos(), routingKey, evento);
            log.info("[RABBIT] Evento publicado. tipo={} pedido={} routingKey={}",
                    evento.tipo(), evento.numeroPedido(), routingKey);
        } catch (AmqpException e) {
            log.error("[RABBIT] No se pudo publicar el evento {} del pedido {}: {}",
                    evento.tipo(), evento.numeroPedido(), e.getMessage());
        }
    }
}
