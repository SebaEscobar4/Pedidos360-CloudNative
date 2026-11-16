package cl.duoc.pedidos360.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * =========================================================================
 * MICROSERVICIO: kafka-admin-service
 * =========================================================================
 * FUNCION: API REST para administrar el cluster Kafka en tiempo de ejecucion:
 *          - Cluster: brokers y controlador.
 *          - Topicos: crear, listar, inspeccionar, modificar configuraciones, ampliar particiones y eliminar.
 *          - Consumer groups: listar, inspeccionar (miembros, offsets y lag), reiniciar offsets y eliminar.
 *          Solo accesible con un JWT valido del IDaaS que tenga el App Role ADMIN.
 */
@SpringBootApplication
public class KafkaAdminServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminServiceApplication.class, args);
    }
}
