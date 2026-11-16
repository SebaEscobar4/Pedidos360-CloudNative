package cl.duoc.pedidos360.analitica.messaging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * =========================================================================
 * MENSAJERIA: ControlReconstruccion
 * =========================================================================
 * FUNCION: Recuerda, por particion, hasta que offset llegaba el consumer group cuando la
 *          particion fue asignada. Los registros anteriores a ese offset ya se habian procesado
 *          antes: se vuelven a leer solo para reconstruir las metricas en memoria.
 *          Si uno de ellos falla, ya fue enviado al DLT la primera vez y no se duplica.
 * CONECTA CON: PedidoEventosListener (marca) y KafkaConsumerConfig (consulta antes de enviar al DLT).
 */
@Component
public class ControlReconstruccion {

    private final Map<TopicPartition, Long> procesadoHasta = new ConcurrentHashMap<>();

    public void marcar(Map<TopicPartition, Long> posicionesComprometidas) {
        procesadoHasta.putAll(posicionesComprometidas);
    }

    public boolean esReproceso(ConsumerRecord<?, ?> registro) {
        Long limite = procesadoHasta.get(new TopicPartition(registro.topic(), registro.partition()));
        return limite != null && registro.offset() < limite;
    }
}
