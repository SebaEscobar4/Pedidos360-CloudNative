package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.Map;

import jakarta.validation.constraints.NotEmpty;

/**
 * Cuerpo de PUT /api/topics/{name}/configs: configuraciones a establecer (las demas no cambian).
 * Ejemplo: { "configs": { "retention.ms": "259200000", "min.insync.replicas": "2" } }
 */
public record ConfiguracionesRequest(
        @NotEmpty(message = "Debe indicar al menos una configuracion en 'configs'")
        Map<String, String> configs) {}
