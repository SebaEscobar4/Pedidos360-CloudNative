package cl.duoc.pedidos360.messaging;

import java.time.LocalDateTime;
import java.util.List;

import cl.duoc.pedidos360.model.Pedido;

/**
 * =========================================================================
 * EVENTO: PedidoEvento
 * =========================================================================
 * FUNCION: Contrato del mensaje que se publica en RabbitMQ cada vez que un pedido
 *          se crea o cambia de estado. Es una copia plana (sin entidades JPA) para
 *          que los microservicios consumidores no dependan de la base de datos.
 * CONECTA CON: PedidoService (lo emite) y PedidoEventPublisher (lo envia al broker).
 */
public record PedidoEvento(
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
