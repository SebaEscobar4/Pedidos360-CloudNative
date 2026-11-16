package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Cuerpo de PATCH /api/topics/{name}/partitions. Kafka solo permite AUMENTAR las particiones.
 */
public record ParticionesRequest(
        @NotNull(message = "partitions es obligatorio")
        @Min(value = 1, message = "partitions debe ser al menos 1")
        Integer partitions) {}
