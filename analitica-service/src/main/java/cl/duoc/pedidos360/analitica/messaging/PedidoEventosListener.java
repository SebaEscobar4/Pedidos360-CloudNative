package cl.duoc.pedidos360.analitica.messaging;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka;
import cl.duoc.pedidos360.analitica.evento.ValidadorEventos;
import cl.duoc.pedidos360.analitica.metricas.MetricasVentasService;

/**
 * =========================================================================
 * CONSUMIDOR KAFKA: PedidoEventosListener
 * =========================================================================
 * FUNCION: Escucha 'pedidos360.pedidos.eventos' en el consumer group 'pedidos360-analitica'.
 *          Solo orquesta: validar -> calcular metricas (dominio) -> publicar la foto del dia.
 *          No maneja errores aqui: cualquier excepcion la resuelve el DefaultErrorHandler
 *          configurado en KafkaConsumerConfig (reintentos con backoff o envio al DLT).
 *
 *  Resiliencia (ConsumerSeekAware):
 *    - Al RECIBIR una particion (arranque o rebalanceo) se descarta cualquier estado previo y
 *      se vuelve al inicio de la particion: las metricas se reconstruyen exactas a partir del
 *      log de eventos (retencion de 7 dias). Kafka es la fuente de verdad, no la memoria.
 *    - Al PERDER una particion se descarta su estado: ahora la calcula otra instancia del grupo.
 *
 * CONECTA CON: ValidadorEventos, MetricasVentasService y MetricasVentasPublisher.
 */
@Component
public class PedidoEventosListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventosListener.class);

    private final ValidadorEventos validador;
    private final MetricasVentasService metricasService;
    private final MetricasVentasPublisher publisher;
    private final ControlReconstruccion control;

    public PedidoEventosListener(ValidadorEventos validador, MetricasVentasService metricasService,
            MetricasVentasPublisher publisher, ControlReconstruccion control) {
        this.validador = validador;
        this.metricasService = metricasService;
        this.publisher = publisher;
        this.control = control;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // 'assignments' trae la posicion comprometida del grupo: lo anterior ya se proceso antes
        control.marcar(assignments);
        metricasService.descartar(ids(assignments.keySet()));
        callback.seekToBeginning(assignments.keySet());
        log.info("[ANALITICA] Particiones asignadas {}: se reconstruyen desde el inicio", assignments.keySet());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        metricasService.descartar(ids(partitions));
    }

    @KafkaListener(
            id = "analitica-pedidos",
            topics = "${app.kafka.topics.pedidos-eventos.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "pedidoEventoListenerFactory")
    public void onPedidoEvento(
            @Payload PedidoEventoKafka evento,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
            @Header(KafkaHeaders.OFFSET) long offset) {

        validador.validar(evento);
        log.info("[ANALITICA] Evento recibido. tipo={} pedido={} estado={} particion={} offset={}",
                evento.tipo(), evento.numeroPedido(), evento.estado(), particion, offset);

        MetricasVentasService.Resultado resultado = metricasService.registrar(evento, particion);
        // Tambien se publica si el evento era duplicado: asegura que la ultima foto quede en Kafka
        publisher.publicar(resultado.metrica());
    }

    private static Collection<Integer> ids(Collection<TopicPartition> particiones) {
        return particiones.stream().map(TopicPartition::partition).toList();
    }
}
