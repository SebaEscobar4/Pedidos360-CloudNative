package cl.duoc.pedidos360.analitica.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.analitica.metricas.MetricaVentasDia;

/**
 * =========================================================================
 * CONFIGURACION: KafkaProducerConfig
 * =========================================================================
 * FUNCION: Define los dos productores del microservicio:
 *
 *   metricasKafkaTemplate  KafkaTemplate<String, MetricaVentasDia>
 *                          clave = fecha (StringSerializer), valor = DTO en JSON (JsonSerializer).
 *
 *   dltKafkaTemplate       Lo usa DeadLetterPublishingRecoverer para copiar un mensaje fallido a su
 *                          Dead Letter Topic. Debe serializar dos tipos de valor:
 *                            - byte[]  -> el cuerpo original, cuando el JSON no se pudo deserializar
 *                            - objeto  -> el DTO ya leido, cuando fallo la validacion o el procesamiento
 *                          por eso usa DelegatingByTypeSerializer.
 * CONECTA CON: MetricasVentasPublisher y KafkaConsumerConfig (manejo de errores).
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    KafkaTemplate<String, MetricaVentasDia> metricasKafkaTemplate(KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        DefaultKafkaProducerFactory<String, MetricaVentasDia> factory = new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(null),
                new StringSerializer(),
                new JsonSerializer<MetricaVentasDia>(objectMapper).noTypeInfo());
        return new KafkaTemplate<>(factory);
    }

    @Bean
    KafkaTemplate<Object, Object> dltKafkaTemplate(KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        Map<Class<?>, Serializer<?>> claves = new LinkedHashMap<>();
        claves.put(byte[].class, new ByteArraySerializer());
        claves.put(String.class, new StringSerializer());

        Map<Class<?>, Serializer<?>> valores = new LinkedHashMap<>();
        valores.put(byte[].class, new ByteArraySerializer());
        valores.put(String.class, new StringSerializer());
        valores.put(Object.class, new JsonSerializer<>(objectMapper).noTypeInfo());

        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(null),
                new DelegatingByTypeSerializer(claves, true),
                new DelegatingByTypeSerializer(valores, true));
        return new KafkaTemplate<>(factory);
    }
}
