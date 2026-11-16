package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/**
 * Detalle de un consumer group: estado, coordinador, miembros con sus particiones asignadas
 * y, por cada particion, el offset confirmado, el ultimo offset del topico y el lag (pendientes).
 */
public record GrupoDetalle(
        String groupId,
        String state,
        Integer coordinator,
        String assignor,
        List<Miembro> members,
        List<OffsetParticion> offsets,
        long totalLag) {

    public record Miembro(String consumerId, String clientId, String host, List<String> assignments) {}

    public record OffsetParticion(String topic, int partition, Long committedOffset, long endOffset, Long lag) {}
}
