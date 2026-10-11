package cl.duoc.pedidos360.rabbitadmin.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * =========================================================================
 * DTO: QueueRequest
 * =========================================================================
 * FUNCION: Datos para crear una cola. Las anotaciones impiden nombres vacios,
 *          caracteres extranos y configuraciones invalidas antes de llegar a RabbitMQ.
 *
 *  name                 (obligatorio) nombre de la cola.
 *  durable              (opcional, por defecto true) sobrevive a reinicios del broker.
 *  deadLetterExchange   (opcional) exchange al que van los mensajes rechazados.
 *  deadLetterRoutingKey (opcional) routing key usada al enviar a la DLQ; exige deadLetterExchange.
 *  messageTtl           (opcional) milisegundos de vida de cada mensaje.
 *  maxLength            (opcional) cantidad maxima de mensajes en la cola.
 */
public record QueueRequest(

        @NotBlank(message = "el nombre de la cola es obligatorio")
        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "el nombre no puede superar los 255 caracteres")
        @Pattern(regexp = NombresRabbit.PATRON, message = "el nombre " + NombresRabbit.MENSAJE_PATRON)
        String name,

        Boolean durable,

        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "deadLetterExchange no puede superar los 255 caracteres")
        @Pattern(regexp = NombresRabbit.PATRON, message = "deadLetterExchange " + NombresRabbit.MENSAJE_PATRON)
        String deadLetterExchange,

        @Size(max = NombresRabbit.LARGO_MAXIMO, message = "deadLetterRoutingKey no puede superar los 255 caracteres")
        String deadLetterRoutingKey,

        @Positive(message = "messageTtl debe ser mayor a 0 (milisegundos)")
        @Max(value = 86_400_000L, message = "messageTtl no puede superar las 24 horas (86400000 ms)")
        Long messageTtl,

        @Positive(message = "maxLength debe ser mayor a 0")
        @Max(value = 1_000_000, message = "maxLength no puede superar 1000000 mensajes")
        Integer maxLength) {

    public boolean durableOrDefault() {
        return durable == null || durable;
    }

    @JsonIgnore
    @AssertTrue(message = "deadLetterRoutingKey requiere que tambien se indique deadLetterExchange")
    public boolean isDeadLetterCoherente() {
        boolean tieneRoutingKey = deadLetterRoutingKey != null && !deadLetterRoutingKey.isBlank();
        boolean tieneExchange = deadLetterExchange != null && !deadLetterExchange.isBlank();
        return !tieneRoutingKey || tieneExchange;
    }
}
