package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/** Estado del cluster: identificador, broker controlador y brokers en linea. */
public record ClusterResponse(String clusterId, Integer controllerId, List<Broker> brokers) {

    public record Broker(int id, String host, int port, String rack) {}
}
