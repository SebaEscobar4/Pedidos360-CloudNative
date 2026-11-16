package cl.duoc.pedidos360;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import cl.duoc.pedidos360.dto.MetricaVentasDTO;
import cl.duoc.pedidos360.service.MetricasVentasStore;

/**
 * Pruebas del endpoint de metricas que alimenta Kafka: seguridad por rol (401/403),
 * listado ordenado (200) y dia inexistente (404). No necesita un cluster Kafka:
 * la vista se carga directamente en MetricasVentasStore.
 */
@SpringBootTest
@AutoConfigureMockMvc
class AdminMetricasApiTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private MetricasVentasStore store;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    @DisplayName("GET /api/admin/metricas sin token responde 401 y sin rol ADMIN responde 403")
    void metricasExigenRolAdmin() throws Exception {
        mockMvc.perform(get("/api/admin/metricas")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/admin/metricas").with(jwt())).andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("GET /api/admin/metricas entrega las metricas recibidas por Kafka, la mas reciente primero")
    void listaMetricas() throws Exception {
        // Fotos parciales por particion, como llegan desde Kafka: el endpoint suma las de un mismo dia
        store.guardar("2026-11-14#p0", metrica(LocalDate.of(2026, 11, 14), 0, 3, 30000));
        store.guardar("2026-11-15#p0", metrica(LocalDate.of(2026, 11, 15), 0, 1, 5000));
        store.guardar("2026-11-15#p2", metrica(LocalDate.of(2026, 11, 15), 2, 1, 10000));

        mockMvc.perform(get("/api/admin/metricas").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].fecha").value("2026-11-15"))
                .andExpect(jsonPath("$[0].totalPedidos").value(2))
                .andExpect(jsonPath("$[0].montoTotal").value(15000.0))
                .andExpect(jsonPath("$[0].ticketPromedio").value(7500.0))
                .andExpect(jsonPath("$[0].pedidosPorEstado.PENDIENTE").value(2))
                .andExpect(jsonPath("$[0].unidadesPorProducto.Pizza").value(2))
                .andExpect(jsonPath("$[1].fecha").value("2026-11-14"));

        mockMvc.perform(get("/api/admin/metricas/2026-11-14").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.montoTotal").value(30000.0));
        mockMvc.perform(get("/api/admin/metricas/2020-01-01").with(admin()))
                .andExpect(status().isNotFound());
    }

    private static org.springframework.test.web.servlet.request.RequestPostProcessor admin() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    private static MetricaVentasDTO metrica(LocalDate fecha, int particion, long pedidos, double monto) {
        return new MetricaVentasDTO(fecha, particion, pedidos, monto, monto / pedidos,
                Map.of("PENDIENTE", pedidos), Map.of("Pizza", pedidos), "Pizza", pedidos, LocalDateTime.now());
    }
}
