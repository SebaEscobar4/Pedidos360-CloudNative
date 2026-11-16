package cl.duoc.pedidos360.analitica;

import java.util.TimeZone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * =========================================================================
 * MICROSERVICIO: analitica-service
 * =========================================================================
 * FUNCION: Analitica de ventas en streaming con Kafka:
 *          - CONSUME 'pedidos360.pedidos.eventos' (consumer group 'pedidos360-analitica').
 *          - Calcula metricas por dia: pedidos, monto, ticket promedio, estados y productos.
 *          - PRODUCE la foto actualizada en 'pedidos360.metricas.ventas' (topico compactado).
 *          - Envia a Dead Letter Topics los eventos que no puede procesar y los registra en el log.
 */
@SpringBootApplication
public class AnaliticaServiceApplication {

    public static void main(String[] args) {
        // Las metricas y los registros usan la hora de Chile aunque el servidor este en UTC
        TimeZone.setDefault(TimeZone.getTimeZone(
                System.getenv().getOrDefault("APP_TIMEZONE", "America/Santiago")));
        SpringApplication.run(AnaliticaServiceApplication.class, args);
    }
}
