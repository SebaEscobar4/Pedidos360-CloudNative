package cl.duoc.pedidos360.analitica.messaging;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.analitica.config.KafkaAnaliticaProperties;
import cl.duoc.pedidos360.analitica.metricas.MetricaVentasDia;

/**
 * =========================================================================
 * PRODUCTOR KAFKA: MetricasVentasPublisher
 * =========================================================================
 * FUNCION: Publica la foto actualizada de las metricas de un dia en 'pedidos360.metricas.ventas'.
 *          Clave = fecha: el topico compactado conserva solo la ultima foto de cada dia.
 *
 *          El envio se espera (con limite de tiempo) a proposito: si falla, la excepcion vuelve al
 *          contenedor del listener, que reintenta el evento de pedido. Como el servicio de metricas
 *          es idempotente, el reintento no cuenta dos veces el mismo pedido; solo vuelve a publicar.
 * CONECTA CON: PedidoEventosListener.
 */
@Component
public class MetricasVentasPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetricasVentasPublisher.class);
    private static final long ESPERA_MAXIMA_SEGUNDOS = 10;

    private final KafkaTemplate<String, MetricaVentasDia> kafkaTemplate;
    private final String topico;

    public MetricasVentasPublisher(KafkaTemplate<String, MetricaVentasDia> metricasKafkaTemplate, KafkaAnaliticaProperties props) {
        this.kafkaTemplate = metricasKafkaTemplate;
        this.topico = props.topics().metricasVentas().name();
    }

    public void publicar(MetricaVentasDia metrica) {
        try {
            var metadata = kafkaTemplate.send(topico, metrica.clave(), metrica)
                    .get(ESPERA_MAXIMA_SEGUNDOS, TimeUnit.SECONDS)
                    .getRecordMetadata();
            log.info("[ANALITICA] Metricas {} publicadas: pedidos={} monto={} estados={} -> {}-{}@{}",
                    metrica.clave(), metrica.totalPedidos(), Math.round(metrica.montoTotal()), metrica.pedidosPorEstado(),
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Publicacion de metricas interrumpida", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("No se pudieron publicar las metricas del " + metrica.fecha(), e);
        }
    }
}
