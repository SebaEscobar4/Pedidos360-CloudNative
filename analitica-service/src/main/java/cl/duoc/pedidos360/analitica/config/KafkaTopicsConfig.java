package cl.duoc.pedidos360.analitica.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * =========================================================================
 * CONFIGURACION: KafkaTopicsConfig
 * =========================================================================
 * FUNCION: Declara los topicos que este microservicio PRODUCE. Al iniciar, KafkaAdmin los
 *          crea si no existen con sus particiones, replicas y configuraciones (compactacion,
 *          retencion, min.insync.replicas). El topico de pedidos lo administra el backend.
 * CONECTA CON: KafkaAnaliticaProperties.
 */
@Configuration
@EnableConfigurationProperties(KafkaAnaliticaProperties.class)
public class KafkaTopicsConfig {

    @Bean
    NewTopic metricasVentasTopic(KafkaAnaliticaProperties props) {
        return construir(props.topics().metricasVentas());
    }

    @Bean
    NewTopic metricasVentasDltTopic(KafkaAnaliticaProperties props) {
        return construir(props.topics().metricasVentasDlt());
    }

    private static NewTopic construir(KafkaAnaliticaProperties.Topico topico) {
        return TopicBuilder.name(topico.name())
                .partitions(topico.partitions())
                .replicas(topico.replicas())
                .configs(topico.configs())
                .build();
    }
}
