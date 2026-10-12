package cl.duoc.pedidos360;

import java.util.TimeZone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * =========================================================================
 * CLASE PRINCIPAL: Pedidos360BackendApplication
 * =========================================================================
 * FUNCION: Punto de entrada principal para iniciar el backend Spring Boot.
 *          La anotacion @SpringBootApplication activa:
 *          1. @Configuration: Permite registrar componentes y beans.
 *          2. @EnableAutoConfiguration: Configura Tomcat, Spring Security y JPA automaticamente.
 *          3. @ComponentScan: Escanea todos los paquetes hijos para registrar Controllers, Services y Repositories.
 */
@SpringBootApplication
public class Pedidos360BackendApplication {

    public static void main(String[] args) {
        // Las fechas de los pedidos usan la hora de Chile aunque el servidor cloud este en UTC
        TimeZone.setDefault(TimeZone.getTimeZone(
                System.getenv().getOrDefault("APP_TIMEZONE", "America/Santiago")));
        SpringApplication.run(Pedidos360BackendApplication.class, args);
    }
}
