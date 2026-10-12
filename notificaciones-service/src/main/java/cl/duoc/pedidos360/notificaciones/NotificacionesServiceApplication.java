package cl.duoc.pedidos360.notificaciones;

import java.util.TimeZone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * =========================================================================
 * MICROSERVICIO: notificaciones-service
 * =========================================================================
 * FUNCION: Procesa de forma asincrona los eventos de pedidos publicados en RabbitMQ:
 *          - Dominio 'notificacion': avisa al cliente cuando su pedido se crea o cambia de estado.
 *          - Dominio 'ticket': genera el ticket de compra de cada pedido nuevo.
 *          - Dominio 'dlq': registra en el log los mensajes que terminaron en una Dead Letter Queue.
 */
@SpringBootApplication
public class NotificacionesServiceApplication {

    public static void main(String[] args) {
        // Los registros usan la hora de Chile aunque el servidor cloud este en UTC
        TimeZone.setDefault(TimeZone.getTimeZone(
                System.getenv().getOrDefault("APP_TIMEZONE", "America/Santiago")));
        SpringApplication.run(NotificacionesServiceApplication.class, args);
    }
}
