package cl.duoc.pedidos360.analitica.config;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import com.fasterxml.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.analitica.evento.EventoInvalidoException;
import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka;
import cl.duoc.pedidos360.analitica.messaging.ControlReconstruccion;

/**
 * =========================================================================
 * CONFIGURACION: KafkaConsumerConfig
 * =========================================================================
 * FUNCION: Infraestructura de consumo del microservicio. La logica de negocio no aparece aqui.
 *
 *  1. Deserializacion segura: ErrorHandlingDeserializer envuelve a JsonDeserializer. Si llega un
 *     cuerpo que no es JSON valido, el consumidor NO entra en un bucle de errores: el registro
 *     se entrega al manejador de errores como DeserializationException.
 *
 *  2. Manejo de errores (DefaultErrorHandler):
 *       - Error transitorio      -> reintentos con espera exponencial (1s, 2s, 4s).
 *       - Error no recuperable   -> sin reintentos (JSON ilegible o EventoInvalidoException).
 *       - Reintentos agotados    -> DeadLetterPublishingRecoverer copia el mensaje al topico .DLT
 *                                   con cabeceras de la excepcion, topico, particion y offset de origen.
 *     Cada intento y cada envio al DLT queda en el log con su contexto (topico-particion@offset, clave).
 *     El offset se confirma despues de enviar al DLT: un mensaje defectuoso nunca detiene la particion.
 *
 *  3. Consumer group 'pedidos360-analitica' con concurrencia 3 (un hilo por particion).
 *
 * CONECTA CON: PedidoEventosListener y DeadLetterListener (via containerFactory).
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // ------------------------------------------------- Eventos de pedidos

    @Bean
    DefaultKafkaConsumerFactory<String, PedidoEventoKafka> pedidoEventoConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        // El tipo destino es fijo (no se leen cabeceras de tipo): el productor no depende de clases de este servicio
        JsonDeserializer<PedidoEventoKafka> json = new JsonDeserializer<>(PedidoEventoKafka.class, objectMapper, false);
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(null),
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(json));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PedidoEventoKafka> pedidoEventoListenerFactory(
            DefaultKafkaConsumerFactory<String, PedidoEventoKafka> pedidoEventoConsumerFactory,
            DefaultErrorHandler pedidoEventoErrorHandler,
            KafkaAnaliticaProperties props) {
        ConcurrentKafkaListenerContainerFactory<String, PedidoEventoKafka> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(pedidoEventoConsumerFactory);
        factory.setConcurrency(props.consumo().concurrencia());
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        factory.setCommonErrorHandler(pedidoEventoErrorHandler);
        return factory;
    }

    @Bean
    DefaultErrorHandler pedidoEventoErrorHandler(KafkaTemplate<Object, Object> dltKafkaTemplate, KafkaAnaliticaProperties props,
            ControlReconstruccion control) {
        Map<String, String> destinos = Map.of(
                props.topics().pedidosEventos().name(), props.topics().pedidosEventosDlt().name(),
                props.topics().metricasVentas().name(), props.topics().metricasVentasDlt().name());

        // Particion -1: Kafka elige la particion del DLT (el DLT tiene menos particiones que el topico original)
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (registro, ex) -> new TopicPartition(destinos.getOrDefault(registro.topic(), registro.topic() + ".DLT"), -1));

        var reintentos = props.reintentos();
        ExponentialBackOff backOff = new ExponentialBackOffWithMaxRetries(reintentos.maximo());
        backOff.setInitialInterval(reintentos.intervaloInicialMs());
        backOff.setMultiplier(reintentos.multiplicador());
        backOff.setMaxInterval(reintentos.intervaloMaximoMs());

        DefaultErrorHandler handler = new DefaultErrorHandler((registro, ex) -> {
            if (control.esReproceso(registro)) {
                // Registro releido para reconstruir metricas: ya se envio al DLT cuando se proceso por primera vez
                log.info("[KAFKA-DLT] {} ya estaba en el DLT (reconstruccion), se omite. motivo={}",
                        contexto(registro), causaRaiz(ex).getMessage());
                return;
            }
            log.error("[KAFKA-DLT] Enviando a '{}' el registro {} clave={} motivo={}",
                    destinos.getOrDefault(registro.topic(), registro.topic() + ".DLT"),
                    contexto(registro), registro.key(), causaRaiz(ex).getMessage());
            dlt.accept(registro, ex);
        }, backOff);

        // Reintentar no arregla un payload invalido: va directo al DLT
        handler.addNotRetryableExceptions(EventoInvalidoException.class);
        handler.setRetryListeners((registro, ex, intento) -> log.warn(
                "[KAFKA-REINTENTO] Intento {} (maximo {}) fallido en {} clave={} motivo={}",
                intento, reintentos.maximo() + 1, contexto(registro), registro.key(), causaRaiz(ex).getMessage()));
        handler.setCommitRecovered(true);
        return handler;
    }

    // ------------------------------------------------- Dead Letter Topic

    /**
     * El DLT se lee como texto: puede contener cuerpos que no son JSON valido.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> dltListenerFactory(
            KafkaProperties kafkaProperties, KafkaAnaliticaProperties props) {
        Map<String, Object> config = kafkaProperties.buildConsumerProperties(null);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, props.consumo().grupoDlt());

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer()));
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        return factory;
    }

    // ------------------------------------------------- Utilidades de log

    static String contexto(ConsumerRecord<?, ?> registro) {
        return registro.topic() + "-" + registro.partition() + "@" + registro.offset();
    }

    static Throwable causaRaiz(Throwable ex) {
        Throwable actual = ex;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual;
    }
}
