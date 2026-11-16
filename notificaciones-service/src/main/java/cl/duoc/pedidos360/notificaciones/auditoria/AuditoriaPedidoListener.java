package cl.duoc.pedidos360.notificaciones.auditoria;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * =========================================================================
 * CONSUMIDOR KAFKA: AuditoriaPedidoListener (dominio 'auditoria')
 * =========================================================================
 * FUNCION: Escucha 'pedidos360.pedidos.eventos' en el consumer group 'pedidos360-auditoria'.
 *          Al ser un grupo distinto al de analitica-service, ambos reciben TODOS los eventos:
 *          cada microservicio cumple su responsabilidad sin quitarle mensajes al otro.
 *          Solo traduce el registro Kafka a una llamada de negocio; los errores los resuelve
 *          el DefaultErrorHandler de KafkaAuditoriaConfig (reintentos o DLT).
 * CONECTA CON: AuditoriaService.
 */
@Component
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AuditoriaPedidoListener {

    private final AuditoriaService auditoriaService;

    public AuditoriaPedidoListener(AuditoriaService auditoriaService) {
        this.auditoriaService = auditoriaService;
    }

    @KafkaListener(
            id = "auditoria-pedidos",
            topics = "${app.kafka.topics.pedidos-eventos}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "auditoriaListenerFactory")
    public void onPedidoEvento(
            @Payload PedidoEvento evento,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topico,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
            @Header(KafkaHeaders.OFFSET) long offset) {
        auditoriaService.registrar(evento, topico + "-" + particion + "@" + offset);
    }
}
