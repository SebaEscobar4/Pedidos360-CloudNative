package cl.duoc.pedidos360.analitica.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * =========================================================================
 * CONFIGURACION: KafkaAnaliticaProperties
 * =========================================================================
 * FUNCION: Mapea el bloque 'app.kafka' de application.yml. Es la unica fuente de nombres
 *          de topicos, parametros de administracion, concurrencia y politica de reintentos.
 * CONECTA CON: KafkaTopicsConfig, KafkaConsumerConfig, KafkaProducerConfig y los listeners.
 */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaAnaliticaProperties(Topicos topics, Consumo consumo, Reintentos reintentos, Idempotencia idempotencia) {

    public record Topicos(Topico pedidosEventos, Topico pedidosEventosDlt, Topico metricasVentas, Topico metricasVentasDlt) {}

    /** partitions, replicas y configs solo se usan en los topicos que este microservicio crea. */
    public record Topico(String name, Integer partitions, Short replicas, Map<String, String> configs) {

        public Topico {
            configs = configs == null ? Map.of() : Map.copyOf(configs);
        }
    }

    public record Consumo(int concurrencia, String grupoDlt) {}

    public record Reintentos(int maximo, long intervaloInicialMs, double multiplicador, long intervaloMaximoMs) {}

    public record Idempotencia(int capacidad) {}
}
