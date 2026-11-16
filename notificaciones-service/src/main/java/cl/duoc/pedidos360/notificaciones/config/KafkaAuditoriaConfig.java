package cl.duoc.pedidos360.notificaciones.config;

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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.notificaciones.messaging.ErrorNoRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * =========================================================================
 * CONFIGURACION: KafkaAuditoriaConfig
 * =========================================================================
 * FUNCION: Infraestructura del consumidor Kafka de auditoria (separada de la de RabbitMQ):
 *   - ErrorHandlingDeserializer + JsonDeserializer al DTO PedidoEvento.
 *   - Consumer group 'pedidos360-auditoria' con 3 hilos (uno por particion).
 *   - DefaultErrorHandler: error transitorio -> reintentos 1s, 2s, 4s; error no recuperable
 *     (payload invalido o JSON ilegible) -> directo al DLT, con log de topico-particion@offset.
 * Solo se activa con app.kafka.enabled=true.
 * CONECTA CON: AuditoriaPedidoListener.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(KafkaAuditoriaProperties.class)
public class KafkaAuditoriaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaAuditoriaConfig.class);

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PedidoEvento> auditoriaListenerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper, KafkaAuditoriaProperties props) {
        JsonDeserializer<PedidoEvento> json = new JsonDeserializer<>(PedidoEvento.class, objectMapper, false);
        DefaultKafkaConsumerFactory<String, PedidoEvento> consumerFactory = new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(null),
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(json));

        ConcurrentKafkaListenerContainerFactory<String, PedidoEvento> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.consumo().concurrencia());
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler(kafkaProperties, objectMapper, props));
        return factory;
    }

    private DefaultErrorHandler errorHandler(KafkaProperties kafkaProperties, ObjectMapper objectMapper,
            KafkaAuditoriaProperties props) {
        String dlt = props.topics().pedidosEventosDlt();
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltTemplate(kafkaProperties, objectMapper), (registro, ex) -> new TopicPartition(dlt, -1));

        var reintentos = props.reintentos();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(reintentos.maximo());
        backOff.setInitialInterval(reintentos.intervaloInicialMs());
        backOff.setMultiplier(reintentos.multiplicador());

        DefaultErrorHandler handler = new DefaultErrorHandler((registro, ex) -> {
            log.error("[AUDITORIA-DLT] Enviando a '{}' el registro {}-{}@{} clave={} motivo={}",
                    dlt, registro.topic(), registro.partition(), registro.offset(), registro.key(), causaRaiz(ex).getMessage());
            recoverer.accept(registro, ex);
        }, backOff);
        handler.addNotRetryableExceptions(ErrorNoRecuperableException.class);
        handler.setRetryListeners((registro, ex, intento) -> log.warn(
                "[AUDITORIA-REINTENTO] Intento {} fallido en {}-{}@{} clave={} motivo={}",
                intento, registro.topic(), registro.partition(), registro.offset(), registro.key(), causaRaiz(ex).getMessage()));
        return handler;
    }

    // El DLT recibe tanto cuerpos ilegibles (byte[]) como eventos ya leidos (JSON)
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

    private static Throwable causaRaiz(Throwable ex) {
        Throwable actual = ex;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual;
    }
}
