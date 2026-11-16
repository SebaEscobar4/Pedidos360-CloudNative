package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Cuerpo de POST /api/consumer-groups/{groupId}/reset-offsets.
 *   destino EARLIEST -> el grupo vuelve a leer el topico desde el principio (reprocesar).
 *   destino LATEST   -> el grupo salta los mensajes pendientes.
 *   destino OFFSET   -> todas las particiones quedan en 'offset' (acotado al rango disponible).
 * Kafka exige que el grupo no tenga consumidores activos.
 */
public record ResetOffsetsRequest(
        @NotBlank(message = "topic es obligatorio") String topic,
        @NotNull(message = "destino es obligatorio (EARLIEST, LATEST u OFFSET)") Destino destino,
        @PositiveOrZero(message = "offset no puede ser negativo") Long offset) {

    public enum Destino { EARLIEST, LATEST, OFFSET }
}
