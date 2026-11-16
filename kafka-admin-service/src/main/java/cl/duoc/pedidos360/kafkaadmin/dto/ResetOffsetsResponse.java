package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.Map;

/** Resultado del reinicio de offsets: nuevo offset de cada particion. */
public record ResetOffsetsResponse(String groupId, String topic, String destino, Map<Integer, Long> nuevosOffsets) {}
