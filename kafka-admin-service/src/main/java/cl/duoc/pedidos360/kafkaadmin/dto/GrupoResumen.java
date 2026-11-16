package cl.duoc.pedidos360.kafkaadmin.dto;

/** Fila del listado GET /api/consumer-groups. */
public record GrupoResumen(String groupId, String state) {}
