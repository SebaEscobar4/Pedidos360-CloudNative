package cl.duoc.pedidos360.analitica.controller;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoint publico para verificar que el microservicio esta arriba.
 * Las metricas no se exponen aqui: el backend las lee desde Kafka y las entrega con JWT (rol ADMIN).
 */
@RestController
public class HealthController {

    @GetMapping("/public/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "system", "Pedidos360 Analitica - Seccion 002D");
    }
}
