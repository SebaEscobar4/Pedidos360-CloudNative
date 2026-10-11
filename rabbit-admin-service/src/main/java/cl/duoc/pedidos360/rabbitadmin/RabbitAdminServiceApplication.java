package cl.duoc.pedidos360.rabbitadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * =========================================================================
 * MICROSERVICIO: rabbit-admin-service
 * =========================================================================
 * FUNCION: API REST para administrar RabbitMQ en tiempo de ejecucion:
 *          crear, consultar y eliminar colas, exchanges y bindings.
 *          Solo accesible con un JWT valido del IDaaS que tenga el App Role ADMIN.
 */
@SpringBootApplication
public class RabbitAdminServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitAdminServiceApplication.class, args);
    }
}
