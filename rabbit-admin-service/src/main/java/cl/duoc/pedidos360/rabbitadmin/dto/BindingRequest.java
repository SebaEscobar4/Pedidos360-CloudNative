package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * =========================================================================
 * DTO: BindingRequest
 * =========================================================================
 * FUNCION: Identifica un binding cola <-> exchange. Se usa tanto para crearlo (body JSON)
 *          como para eliminarlo (parametros de consulta).
 *
 *  queue      (obligatorio) cola de destino; debe existir.
 *  exchange   (obligatorio) exchange de origen; debe existir.
 *  routingKey (obligatorio, puede ser "" para exchanges fanout).
 */
public record BindingRequest(

        @NotBlank(message = "la cola (queue) es obligatoria")
        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "queue no puede superar los 255 caracteres")
        @Pattern(regexp = NombresRabbit.PATRON, message = "queue " + NombresRabbit.MENSAJE_PATRON)
        String queue,

        @NotBlank(message = "el exchange es obligatorio")
        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "exchange no puede superar los 255 caracteres")
        @Pattern(regexp = NombresRabbit.PATRON, message = "exchange " + NombresRabbit.MENSAJE_PATRON)
        String exchange,

        @NotNull(message = "routingKey es obligatoria (use \"\" para exchanges fanout)")
        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "routingKey no puede superar los 255 caracteres")
        String routingKey) {
}
