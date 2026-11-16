package cl.duoc.pedidos360.kafkaadmin.dto;

/** Fila del listado GET /api/topics. */
public record TopicoResumen(String name, int partitions, int replicationFactor, boolean internal) {}
