package cl.duoc.pedidos360.notificaciones.controller;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoint publico para verificar que el microservicio esta arriba.
 */
@RestController
public class HealthController {

    @GetMapping("/public/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "system", "Pedidos360 Notificaciones - Seccion 002D");
    }
}
