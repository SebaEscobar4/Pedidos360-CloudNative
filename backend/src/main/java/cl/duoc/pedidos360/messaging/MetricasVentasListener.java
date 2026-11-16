package cl.duoc.pedidos360.messaging;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.dto.MetricaVentasDTO;
import cl.duoc.pedidos360.service.MetricasVentasStore;

/**
 * =========================================================================
 * CONSUMIDOR KAFKA: MetricasVentasListener
 * =========================================================================
 * FUNCION: Lee 'pedidos360.metricas.ventas' (topico compactado) y mantiene la vista de
 *          metricas que entrega el panel de administracion.
 *
 *  - ConsumerSeekAware: al recibir sus particiones vuelve al inicio del topico. Como el topico
 *    esta compactado, eso equivale a cargar la ultima foto de cada dia: la vista se reconstruye
 *    completa en cada arranque, sin base de datos adicional.
 *  - Un mensaje con valor nulo (tombstone) significa que esa fecha se elimino.
 *  - Errores: un payload invalido lanza IllegalArgumentException (no recuperable -> DLT), ver KafkaConsumerConfig.
 * CONECTA CON: MetricasVentasStore.
 */
@Component
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MetricasVentasListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(MetricasVentasListener.class);

    private final MetricasVentasStore store;

    public MetricasVentasListener(MetricasVentasStore store) {
        this.store = store;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToBeginning(assignments.keySet());
        log.info("[KAFKA-METRICAS] Reconstruyendo la vista de metricas desde el inicio de {}", assignments.keySet());
    }

    @KafkaListener(
            id = "backend-metricas",
            topics = "${app.kafka.consumo.metricas-ventas}",
            groupId = "${app.kafka.consumo.grupo-metricas}",
            containerFactory = "metricasListenerFactory")
    public void onMetrica(
            @Payload(required = false) MetricaVentasDTO metrica,
            @Header(KafkaHeaders.RECEIVED_KEY) String clave,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
            @Header(KafkaHeaders.OFFSET) long offset) {

        LocalDate fecha = fechaDeClave(clave);
        if (metrica == null) {
            store.eliminar(clave);
            log.info("[KAFKA-METRICAS] Tombstone: se eliminan las metricas parciales {}", clave);
            return;
        }
        if (!fecha.equals(metrica.fecha())) {
            throw new IllegalArgumentException("La metrica del " + metrica.fecha() + " no corresponde a su clave " + clave);
        }
        store.guardar(clave, metrica);
        log.info("[KAFKA-METRICAS] Metricas parciales {} actualizadas: pedidos={} monto={} (particion={} offset={})",
                clave, metrica.totalPedidos(), Math.round(metrica.montoTotal()), particion, offset);
    }

    // Clave esperada: yyyy-MM-dd#pN
    private static LocalDate fechaDeClave(String clave) {
        if (clave == null || !clave.matches("\\d{4}-\\d{2}-\\d{2}#p\\d+")) {
            throw new IllegalArgumentException("Clave de metrica invalida: " + clave);
        }
        try {
            return LocalDate.parse(clave.substring(0, 10));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Clave de metrica invalida: " + clave, e);
        }
    }
}
