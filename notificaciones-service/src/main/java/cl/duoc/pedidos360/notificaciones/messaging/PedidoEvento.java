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
 *          Llega por RabbitMQ (notificacion, ticket) y por Kafka (auditoria).
 * CONECTA CON: Los consumidores de los dominios 'notificacion', 'ticket' y 'auditoria'.
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
        List<Item> items,
        // Identificador unico del evento (lo agrega el backend desde la EP5); null en eventos antiguos
        String eventoId) {

    public static final String TIPO_PEDIDO_CREADO = "PEDIDO_CREADO";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(String productoNombre, Integer cantidad, Double precioUnitario, Double subtotal) {}
}
