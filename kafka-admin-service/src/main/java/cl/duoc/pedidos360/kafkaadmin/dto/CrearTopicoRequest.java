package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Cuerpo de POST /api/topics. Las configuraciones son opcionales y se validan contra una lista
 * de parametros permitidos (ver ValidadorConfiguraciones).
 *
 * Ejemplo:
 * { "name": "pedidos360.reportes.diarios", "partitions": 3, "replicationFactor": 3,
 *   "configs": { "cleanup.policy": "delete", "retention.ms": "86400000", "min.insync.replicas": "2" } }
 */
public record CrearTopicoRequest(
        @NotBlank(message = "El nombre del topico es obligatorio")
        @Size(max = 249, message = "El nombre del topico admite hasta 249 caracteres")
        @Pattern(regexp = "^(?!__)[a-zA-Z0-9._-]+$",
                message = "El nombre solo admite letras, numeros, '.', '_' y '-', y no puede comenzar con '__' (reservado por Kafka)")
        String name,

        @NotNull(message = "partitions es obligatorio")
        @Min(value = 1, message = "partitions debe ser al menos 1")
        Integer partitions,

        @NotNull(message = "replicationFactor es obligatorio")
        @Min(value = 1, message = "replicationFactor debe ser al menos 1")
        @Max(value = 10, message = "replicationFactor no puede superar 10")
        Short replicationFactor,

        Map<String, String> configs) {

    public CrearTopicoRequest {
        configs = configs == null ? Map.of() : configs;
    }
}
