package cl.duoc.pedidos360.controller;

import java.time.LocalDate;
import java.util.List;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.dto.MetricaVentasDTO;
import cl.duoc.pedidos360.service.MetricasVentasStore;

/**
 * =========================================================================
 * CONTROLADOR: AdminMetricasController
 * =========================================================================
 *   GET /api/admin/metricas           Metricas de ventas de todos los dias        200
 *   GET /api/admin/metricas/{fecha}   Metricas de un dia (yyyy-MM-dd)             200 | 404
 * Las metricas las calcula analitica-service en streaming y llegan por Kafka.
 * Exige el App Role ADMIN (regla /api/admin/** de SecurityConfig).
 * CONECTA CON: MetricasVentasStore.
 */
@RestController
@RequestMapping("/api/admin/metricas")
public class AdminMetricasController {

    private final MetricasVentasStore store;

    public AdminMetricasController(MetricasVentasStore store) {
        this.store = store;
    }

    @GetMapping
    public List<MetricaVentasDTO> listar() {
        return store.listar();
    }

    @GetMapping("/{fecha}")
    public ResponseEntity<MetricaVentasDTO> porFecha(@PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fecha) {
        return store.buscar(fecha)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
