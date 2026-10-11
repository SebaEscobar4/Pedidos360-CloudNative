package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * =========================================================================
 * DTO: ExchangeRequest
 * =========================================================================
 * FUNCION: Datos para crear un exchange.
 *
 *  name    (obligatorio) nombre del exchange.
 *  type    (obligatorio) direct | topic | fanout | headers.
 *  durable (opcional, por defecto true) sobrevive a reinicios del broker.
 */
public record ExchangeRequest(

        @NotBlank(message = "el nombre del exchange es obligatorio")
        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "el nombre no puede superar los 255 caracteres")
        @Pattern(regexp = NombresRabbit.PATRON, message = "el nombre " + NombresRabbit.MENSAJE_PATRON)
        String name,

        @NotBlank(message = "el tipo de exchange es obligatorio")
        @Pattern(regexp = "^(direct|topic|fanout|headers)$",
                message = "el tipo debe ser uno de: direct, topic, fanout, headers")
        String type,

        Boolean durable) {

    public boolean durableOrDefault() {
        return durable == null || durable;
    }
}
