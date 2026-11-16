package cl.duoc.pedidos360.kafkaadmin.config;

import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * =========================================================================
 * CONFIGURACION: KafkaAdminConfig
 * =========================================================================
 * FUNCION: Crea el AdminClient de Kafka a partir de la configuracion spring.kafka
 *          (bootstrap servers, timeouts) y expone las propiedades 'app.kafka-admin'.
 * CONECTA CON: KafkaAdminService.
 */
@Configuration
@EnableConfigurationProperties(KafkaAdminConfig.KafkaAdminProperties.class)
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    /**
     * @param timeoutSegundos   espera maxima de cada operacion
     * @param topicosProtegidos topicos que no se pueden eliminar (los usan los microservicios)
     * @param maxParticiones    limite de particiones al crear o ampliar un topico
     */
    @ConfigurationProperties(prefix = "app.kafka-admin")
    public record KafkaAdminProperties(int timeoutSegundos, List<String> topicosProtegidos, int maxParticiones) {

        public KafkaAdminProperties {
            topicosProtegidos = topicosProtegidos == null ? List.of() : List.copyOf(topicosProtegidos);
        }
    }
}
