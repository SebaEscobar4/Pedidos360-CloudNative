package cl.duoc.pedidos360.analitica.messaging;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * =========================================================================
 * CONSUMIDOR KAFKA: DeadLetterListener
 * =========================================================================
 * FUNCION: Lee el Dead Letter Topic de los eventos de pedidos (grupo 'pedidos360-analitica-dlt')
 *          y registra en el log cada mensaje fallido con su origen y motivo, usando las cabeceras
 *          que agrega DeadLetterPublishingRecoverer. El mensaje sigue guardado en el DLT
 *          (retencion 14 dias) para revisarlo en Kafka-UI o reprocesarlo.
 * CONECTA CON: KafkaConsumerConfig (dltListenerFactory).
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    @KafkaListener(
            id = "analitica-dlt",
            topics = "${app.kafka.topics.pedidos-eventos-dlt.name}",
            groupId = "${app.kafka.consumo.grupo-dlt}",
            containerFactory = "dltListenerFactory")
    public void onMensajeMuerto(ConsumerRecord<String, String> registro) {
        // Varios grupos consumen el mismo topico: la cabecera indica cual de ellos no pudo procesarlo
        log.error("[DLT] Mensaje fallido. grupo={} origen={}-{}@{} clave={} excepcion={} motivo={} cuerpo={}",
                cabecera(registro, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                cabecera(registro, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                entero(registro, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                largo(registro, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                registro.key(),
                excepcion(registro),
                motivo(registro),
                registro.value());
    }

    // Si el listener fallo, Spring envuelve la excepcion: se informa la causa real
    private static String excepcion(ConsumerRecord<?, ?> registro) {
        String causa = cabecera(registro, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        return causa != null ? causa : cabecera(registro, KafkaHeaders.DLT_EXCEPTION_FQCN);
    }

    private static String motivo(ConsumerRecord<?, ?> registro) {
        String mensaje = cabecera(registro, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String envoltorio = "threw exception; ";
        return mensaje != null && mensaje.contains(envoltorio)
                ? mensaje.substring(mensaje.indexOf(envoltorio) + envoltorio.length())
                : mensaje;
    }

    private static String cabecera(ConsumerRecord<?, ?> registro, String nombre) {
        Header header = registro.headers().lastHeader(nombre);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    // Particion y offset de origen viajan como enteros binarios
    private static Integer entero(ConsumerRecord<?, ?> registro, String nombre) {
        Header header = registro.headers().lastHeader(nombre);
        return header == null || header.value().length != Integer.BYTES ? null
                : java.nio.ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long largo(ConsumerRecord<?, ?> registro, String nombre) {
        Header header = registro.headers().lastHeader(nombre);
        return header == null || header.value().length != Long.BYTES ? null
                : java.nio.ByteBuffer.wrap(header.value()).getLong();
    }
}
