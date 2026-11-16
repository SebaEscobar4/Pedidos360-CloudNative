package cl.duoc.pedidos360.analitica.evento;

import java.time.LocalDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * =========================================================================
 * DTO: PedidoEventoKafka
 * =========================================================================
 * FUNCION: Copia local y tipada del contrato JSON que el backend publica en
 *          'pedidos360.pedidos.eventos'. Las anotaciones de Bean Validation describen
 *          que es un evento valido; ValidadorEventos las aplica antes de procesarlo.
 *          Ignora campos desconocidos para tolerar que el productor agregue datos nuevos.
 * CONECTA CON: KafkaConsumerConfig (JsonDeserializer), ValidadorEventos y MetricasVentasService.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PedidoEventoKafka(
        @NotBlank(message = "eventoId es obligatorio") String eventoId,
        @NotNull(message = "tipo es obligatorio") TipoEvento tipo,
        Long pedidoId,
        @NotBlank(message = "numeroPedido es obligatorio") String numeroPedido,
        String clienteNombre,
        String clienteEmail,
        @NotBlank(message = "estado es obligatorio") String estado,
        @PositiveOrZero(message = "total no puede ser negativo") Double total,
        LocalDateTime fechaCreacion,
        @NotNull(message = "fechaEvento es obligatoria") LocalDateTime fechaEvento,
        List<@Valid Item> items) {

    public enum TipoEvento { PEDIDO_CREADO, ESTADO_ACTUALIZADO }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(
            @NotBlank(message = "productoNombre es obligatorio") String productoNombre,
            @NotNull @Positive(message = "cantidad debe ser mayor a 0") Integer cantidad,
            @PositiveOrZero Double precioUnitario,
            @PositiveOrZero Double subtotal) {}
}
