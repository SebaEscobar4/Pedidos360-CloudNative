package cl.duoc.pedidos360.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * =========================================================================
 * CONFIGURACION: KafkaTopicosProperties
 * =========================================================================
 * FUNCION: Mapea el bloque 'app.kafka' de application.yml a un objeto tipado.
 *          Es la unica fuente de nombres y parametros de administracion de los topicos
 *          (particiones, factor de replicacion, retencion, cleanup policy, etc.):
 *          ninguna otra clase escribe estos valores como texto literal.
 * CONECTA CON: KafkaProducerConfig (declara los topicos) y PedidoKafkaPublisher (productor).
 */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaTopicosProperties(boolean enabled, Topicos topics, Consumo consumo) {

    public record Topicos(Topico pedidosEventos, Topico pedidosEventosDlt) {}

    /**
     * Lado consumidor del backend: lee las metricas que publica analitica-service.
     *
     * @param metricasVentas    topico compactado de metricas (lo administra analitica-service)
     * @param metricasVentasDlt Dead Letter Topic de ese flujo
     * @param grupoMetricas     consumer group de ESTA instancia del backend
     */
    public record Consumo(String metricasVentas, String metricasVentasDlt, String grupoMetricas) {}

    /**
     * @param name       nombre del topico
     * @param partitions numero de particiones (paralelismo maximo de un consumer group)
     * @param replicas   factor de replicacion (copias en distintos brokers)
     * @param configs    configuraciones de topico: retention.ms, cleanup.policy, min.insync.replicas...
     */
    public record Topico(String name, int partitions, short replicas, Map<String, String> configs) {

        public Topico {
            configs = configs == null ? Map.of() : Map.copyOf(configs);
        }
    }
}
