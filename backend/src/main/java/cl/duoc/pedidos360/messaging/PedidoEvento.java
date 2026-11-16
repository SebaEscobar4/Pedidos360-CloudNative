package cl.duoc.pedidos360.messaging;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import cl.duoc.pedidos360.model.Pedido;

/**
 * =========================================================================
 * EVENTO: PedidoEvento
 * =========================================================================
 * FUNCION: Contrato (DTO) del mensaje que se publica en RabbitMQ y en Kafka cada vez que
 *          un pedido se crea o cambia de estado. Es una copia plana (sin entidades JPA) para
 *          que los microservicios consumidores no dependan de la base de datos.
 *          'eventoId' identifica cada evento de forma unica: los consumidores lo usan para
 *          descartar duplicados (Kafka garantiza entrega "al menos una vez").
 * CONECTA CON: PedidoService (lo emite), PedidoEventPublisher (RabbitMQ) y PedidoKafkaPublisher (Kafka).
 */
public record PedidoEvento(
        String eventoId,
        TipoEvento tipo,
        Long pedidoId,
        String numeroPedido,
        String clienteNombre,
        String clienteEmail,
        String estado,
        Double total,
        LocalDateTime fechaCreacion,
        LocalDateTime fechaEvento,
        List<Item> items) {

    public enum TipoEvento { PEDIDO_CREADO, ESTADO_ACTUALIZADO }

    public record Item(String productoNombre, Integer cantidad, Double precioUnitario, Double subtotal) {}

    public static PedidoEvento desde(TipoEvento tipo, Pedido pedido) {
        List<Item> items = pedido.getItems().stream()
                .map(i -> new Item(i.getProductoNombre(), i.getCantidad(), i.getPrecioUnitario(), i.getSubtotal()))
                .toList();
        return new PedidoEvento(
                UUID.randomUUID().toString(),
                tipo,
                pedido.getId(),
                pedido.getNumeroPedido(),
                pedido.getClienteNombre(),
                pedido.getClienteEmail(),
                pedido.getEstado().name(),
                pedido.getTotal(),
                pedido.getFechaCreacion(),
                LocalDateTime.now(),
                items);
    }
}
