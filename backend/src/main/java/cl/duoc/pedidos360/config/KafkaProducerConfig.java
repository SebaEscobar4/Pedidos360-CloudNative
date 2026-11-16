package cl.duoc.pedidos360.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.messaging.PedidoEvento;

/**
 * =========================================================================
 * CONFIGURACION: KafkaProducerConfig
 * =========================================================================
 * FUNCION: Configura el lado productor de Kafka del backend de pedidos:
 *
 *   1. Topicos como beans NewTopic: al iniciar, KafkaAdmin los crea si no existen con sus
 *      particiones, replicas y configuraciones (retention.ms, cleanup.policy, min.insync.replicas).
 *
 *   2. ProducerFactory + KafkaTemplate tipados para PedidoEvento:
 *        clave  -> String (numero de pedido)        StringSerializer
 *        valor  -> PedidoEvento en JSON             JsonSerializer con el ObjectMapper de Spring
 *      Las propiedades de conexion (bootstrap servers, acks=all, idempotencia, compresion...)
 *      vienen de spring.kafka en application.yml.
 *
 * Solo se activa con app.kafka.enabled=true: si se desactiva, el backend sigue funcionando
 * igual que antes (RabbitMQ no se ve afectado).
 * CONECTA CON: KafkaTopicosProperties y PedidoKafkaPublisher.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(KafkaTopicosProperties.class)
public class KafkaProducerConfig {

    private final KafkaTopicosProperties props;

    public KafkaProducerConfig(KafkaTopicosProperties props) {
        this.props = props;
    }

    // ---------------------------------------------------------------- Topicos

    @Bean
    NewTopic pedidosEventosTopic() {
        return construir(props.topics().pedidosEventos());
    }

    @Bean
    NewTopic pedidosEventosDltTopic() {
        return construir(props.topics().pedidosEventosDlt());
    }

    private static NewTopic construir(KafkaTopicosProperties.Topico topico) {
        return TopicBuilder.name(topico.name())
                .partitions(topico.partitions())
                .replicas(topico.replicas())
                .configs(topico.configs())
                .build();
    }

    // ---------------------------------------------------------------- Productor

    @Bean
    ProducerFactory<String, PedidoEvento> pedidoEventoProducerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        // Mismo ObjectMapper que la API REST: las fechas viajan en formato ISO-8601.
        // noTypeInfo(): el JSON no lleva el nombre de la clase Java, asi cada consumidor usa su propio DTO.
        JsonSerializer<PedidoEvento> valueSerializer = new JsonSerializer<PedidoEvento>(objectMapper).noTypeInfo();
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(null),
                new StringSerializer(),
                valueSerializer);
    }

    @Bean
    KafkaTemplate<String, PedidoEvento> pedidoEventoKafkaTemplate(
            ProducerFactory<String, PedidoEvento> pedidoEventoProducerFactory) {
        KafkaTemplate<String, PedidoEvento> template = new KafkaTemplate<>(pedidoEventoProducerFactory);
        template.setDefaultTopic(props.topics().pedidosEventos().name());
        return template;
    }
}
