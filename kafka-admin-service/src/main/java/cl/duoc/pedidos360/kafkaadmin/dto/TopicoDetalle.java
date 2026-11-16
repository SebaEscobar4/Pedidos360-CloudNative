package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/**
 * Detalle de un topico:
 *   - particiones con su lider, replicas e ISR (replicas en sincronia),
 *   - configuracionPersonalizada: valores definidos para ESTE topico (sobrescriben al broker),
 *   - configuracionEfectiva: valor vigente de los parametros de administracion principales.
 */
public record TopicoDetalle(
        String name,
        boolean internal,
        int partitions,
        int replicationFactor,
        List<Particion> particiones,
        Map<String, String> configuracionPersonalizada,
        Map<String, String> configuracionEfectiva) {

    public record Particion(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {}
}
