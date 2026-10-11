package cl.duoc.pedidos360.rabbitadmin.dto;

/**
 * Estado de una cola: mensajes pendientes y consumidores conectados.
 */
public record QueueResponse(String name, long messageCount, int consumerCount) {
}
