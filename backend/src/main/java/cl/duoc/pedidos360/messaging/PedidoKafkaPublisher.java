package cl.duoc.pedidos360.messaging;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import cl.duoc.pedidos360.config.KafkaTopicosProperties;

/**
 * =========================================================================
 * PRODUCTOR KAFKA: PedidoKafkaPublisher
 * =========================================================================
 * FUNCION: Publica en el topico 'pedidos360.pedidos.eventos' cada evento de pedido
 *          emitido por PedidoService. Convive con PedidoEventPublisher (RabbitMQ):
 *          ambos escuchan el MISMO evento de Spring y ninguno depende del otro.
 *
 *          RabbitMQ -> trabajo puntual (notificar, generar ticket) que se borra al confirmarse.
 *          Kafka    -> log persistente y re-leible de hechos para streaming y analitica.
 *
 *  - Se ejecuta DESPUES del commit: nunca se publica un pedido que no quedo guardado.
 *  - Clave del mensaje = numero de pedido: todos los eventos de un pedido van a la misma
 *    particion y se consumen en orden.
 *  - Cabeceras 'tipo-evento' y 'origen' para filtrar o enrutar sin deserializar el cuerpo.
 *  - El envio es asincrono: el resultado (particion/offset o error) se registra en el log.
 *    Si Kafka no esta disponible, el pedido sigue su curso normal.
 * CONECTA CON: PedidoService (via eventos de Spring) y KafkaProducerConfig (KafkaTemplate).
 */
@Component
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PedidoKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaPublisher.class);

    static final String HEADER_TIPO_EVENTO = "tipo-evento";
    static final String HEADER_ORIGEN = "origen";
    private static final byte[] ORIGEN = "pedidos360-backend".getBytes(StandardCharsets.UTF_8);

    private final KafkaTemplate<String, PedidoEvento> kafkaTemplate;
    private final String topico;

    public PedidoKafkaPublisher(KafkaTemplate<String, PedidoEvento> kafkaTemplate, KafkaTopicosProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topico = props.topics().pedidosEventos().name();
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publicar(PedidoEvento evento) {
        ProducerRecord<String, PedidoEvento> registro = new ProducerRecord<>(topico, evento.numeroPedido(), evento);
        registro.headers()
                .add(HEADER_TIPO_EVENTO, evento.tipo().name().getBytes(StandardCharsets.UTF_8))
                .add(HEADER_ORIGEN, ORIGEN);
        try {
            kafkaTemplate.send(registro).whenComplete((resultado, error) -> {
                if (error != null) {
                    log.error("[KAFKA] No se pudo publicar el evento {} del pedido {} en '{}': {}",
                            evento.tipo(), evento.numeroPedido(), topico, error.getMessage());
                    return;
                }
                var metadata = resultado.getRecordMetadata();
                log.info("[KAFKA] Evento publicado. tipo={} pedido={} eventoId={} topico={} particion={} offset={}",
                        evento.tipo(), evento.numeroPedido(), evento.eventoId(),
                        metadata.topic(), metadata.partition(), metadata.offset());
            });
        } catch (RuntimeException e) {
            // send() puede fallar de inmediato (por ejemplo, sin metadatos del cluster tras max.block.ms)
            log.error("[KAFKA] No se pudo enviar el evento {} del pedido {}: {}",
                    evento.tipo(), evento.numeroPedido(), e.getMessage());
        }
    }
}
