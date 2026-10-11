package cl.duoc.pedidos360.notificaciones.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * =========================================================================
 * CONFIGURACION: RabbitMQProperties
 * =========================================================================
 * FUNCION: Mapea el bloque 'app.rabbitmq' de application.yml a un objeto tipado.
 *          Es la unica fuente de nombres de exchanges, colas y routing keys.
 * CONECTA CON: RabbitMQConfig (declaracion de beans) y ManejadorErroresMensajeria (reintentos).
 */
@ConfigurationProperties(prefix = "app.rabbitmq")
public record RabbitMQProperties(
        Exchanges exchanges,
        Queues queues,
        RoutingKeys routingKeys,
        Reintentos reintentos) {

    public record Exchanges(String pedidos, String deadLetter) {}

    public record Queues(String notificaciones, String tickets, String notificacionesDlq, String ticketsDlq) {}

    public record RoutingKeys(
            String notificacionesBinding,
            String ticketsBinding,
            String notificacionesDlq,
            String ticketsDlq) {}

    public record Reintentos(int maximo, String header) {}
}
