package cl.duoc.pedidos360.notificaciones.messaging;

import java.time.LocalDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * =========================================================================
 * EVENTO: PedidoEvento
 * =========================================================================
 * FUNCION: Copia local del contrato JSON que publica el backend de pedidos.
 *          Ignora campos desconocidos para tolerar que el productor agregue datos nuevos.
 * CONECTA CON: Los consumidores de los dominios 'notificacion' y 'ticket'.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PedidoEvento(
        String tipo,
        Long pedidoId,
        String numeroPedido,
        String clienteNombre,
        String clienteEmail,
        String estado,
        Double total,
        LocalDateTime fechaCreacion,
        LocalDateTime fechaEvento,
        List<Item> items) {

    public static final String TIPO_PEDIDO_CREADO = "PEDIDO_CREADO";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(String productoNombre, Integer cantidad, Double precioUnitario, Double subtotal) {}
}
