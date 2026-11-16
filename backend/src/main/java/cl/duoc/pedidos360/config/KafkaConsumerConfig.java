package cl.duoc.pedidos360.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.dto.MetricaVentasDTO;

/**
 * =========================================================================
 * CONFIGURACION: KafkaConsumerConfig
 * =========================================================================
 * FUNCION: Infraestructura del consumidor de metricas del backend:
 *   - ErrorHandlingDeserializer + JsonDeserializer al DTO MetricaVentasDTO: un JSON ilegible
 *     no bloquea la particion, se entrega al manejador de errores.
 *   - DefaultErrorHandler: 2 reintentos cada 1 s para errores transitorios; payload invalido
 *     (IllegalArgumentException o JSON ilegible) va directo a 'pedidos360.metricas.ventas.DLT'.
 *   - Consumer group propio de cada instancia (ver app.kafka.consumo.grupo-metricas).
 * CONECTA CON: MetricasVentasListener.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, MetricaVentasDTO> metricasListenerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper, KafkaTopicosProperties props) {
        JsonDeserializer<MetricaVentasDTO> json = new JsonDeserializer<>(MetricaVentasDTO.class, objectMapper, false);
        DefaultKafkaConsumerFactory<String, MetricaVentasDTO> consumerFactory = new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(null),
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(json));

        ConcurrentKafkaListenerContainerFactory<String, MetricaVentasDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        factory.setCommonErrorHandler(metricasErrorHandler(kafkaProperties, objectMapper, props));
        return factory;
    }

    private DefaultErrorHandler metricasErrorHandler(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper, KafkaTopicosProperties props) {
        String dlt = props.consumo().metricasVentasDlt();
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltTemplate(kafkaProperties, objectMapper), (registro, ex) -> new TopicPartition(dlt, -1));

        DefaultErrorHandler handler = new DefaultErrorHandler((registro, ex) -> {
            log.error("[KAFKA-METRICAS] Enviando a '{}' el registro {}-{}@{} clave={} motivo={}",
                    dlt, registro.topic(), registro.partition(), registro.offset(), registro.key(), ex.getMessage());
            recoverer.accept(registro, ex);
        }, new FixedBackOff(1000L, 2));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners((registro, ex, intento) -> log.warn(
                "[KAFKA-METRICAS] Intento {} fallido en {}-{}@{}: {}",
                intento, registro.topic(), registro.partition(), registro.offset(), ex.getMessage()));
        return handler;
    }

    // Copia al DLT tanto cuerpos ilegibles (byte[]) como DTO ya leidos (JSON)
    private static KafkaTemplate<Object, Object> dltTemplate(KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        Map<Class<?>, Serializer<?>> claves = new LinkedHashMap<>();
        claves.put(byte[].class, new ByteArraySerializer());
        claves.put(String.class, new StringSerializer());
        Map<Class<?>, Serializer<?>> valores = new LinkedHashMap<>();
        valores.put(byte[].class, new ByteArraySerializer());
        valores.put(Object.class, new JsonSerializer<>(objectMapper).noTypeInfo());
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(null),
                new DelegatingByTypeSerializer(claves, true),
                new DelegatingByTypeSerializer(valores, true)));
    }
}
