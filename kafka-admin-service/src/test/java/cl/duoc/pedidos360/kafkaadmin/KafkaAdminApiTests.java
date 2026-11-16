package cl.duoc.pedidos360.kafkaadmin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import cl.duoc.pedidos360.kafkaadmin.dto.GrupoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoDetalle;
import cl.duoc.pedidos360.kafkaadmin.exception.KafkaNoDisponibleException;
import cl.duoc.pedidos360.kafkaadmin.exception.OperacionNoPermitidaException;
import cl.duoc.pedidos360.kafkaadmin.exception.RecursoNoEncontradoException;
import cl.duoc.pedidos360.kafkaadmin.exception.RecursoYaExisteException;
import cl.duoc.pedidos360.kafkaadmin.exception.SolicitudInvalidaException;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;

/**
 * Pruebas de la API de administracion de Kafka: seguridad (401/403), validacion de entrada (400)
 * y codigos de respuesta (200/201/204/404/409/503). KafkaAdminService se simula, por lo que no
 * se necesita un cluster Kafka para ejecutarlas.
 */
@SpringBootTest
@AutoConfigureMockMvc
class KafkaAdminApiTests {

    private static final String TOPICO_VALIDO = """
            { "name": "pedidos360.reportes.diarios", "partitions": 3, "replicationFactor": 3,
              "configs": { "cleanup.policy": "delete", "retention.ms": "86400000" } }
            """;

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private KafkaAdminService kafkaAdminService;

    // Evita consultar las llaves publicas de Azure AD durante las pruebas
    @MockitoBean
    private JwtDecoder jwtDecoder;

    private static RequestPostProcessor admin() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    private static TopicoDetalle detalle(String nombre, int particiones) {
        return new TopicoDetalle(nombre, false, particiones, 3, List.of(), Map.of(), Map.of());
    }

    @Test
    @DisplayName("1. Sin token responde 401 y con token sin rol ADMIN responde 403")
    void seguridad() throws Exception {
        mockMvc.perform(get("/api/topics")).andExpect(status().isUnauthorized());
        mockMvc.perform(post("/api/topics").with(jwt()).contentType(MediaType.APPLICATION_JSON).content(TOPICO_VALIDO))
                .andExpect(status().isForbidden());
        verifyNoInteractions(kafkaAdminService);
    }

    @Test
    @DisplayName("2. POST /api/topics valido responde 201 con Location")
    void crearTopico_201() throws Exception {
        when(kafkaAdminService.crearTopico(any())).thenReturn(detalle("pedidos360.reportes.diarios", 3));
        mockMvc.perform(post("/api/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content(TOPICO_VALIDO))
                .andExpect(status().isCreated())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers
                        .header().string("Location", "/api/topics/pedidos360.reportes.diarios"))
                .andExpect(jsonPath("$.partitions").value(3));
    }

    @Test
    @DisplayName("3. POST /api/topics con nombre, particiones o replicas invalidas responde 400 sin llegar a Kafka")
    void crearTopico_validacion400() throws Exception {
        mockMvc.perform(post("/api/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"__interno\",\"partitions\":0,\"replicationFactor\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details.length()").value(3));
        mockMvc.perform(post("/api/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content("{ no es json"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(kafkaAdminService);
    }

    @Test
    @DisplayName("4. Topico duplicado responde 409 y configuracion no permitida responde 400")
    void crearTopico_conflictos() throws Exception {
        when(kafkaAdminService.crearTopico(any())).thenThrow(new RecursoYaExisteException("Ya existe el topico"));
        mockMvc.perform(post("/api/topics").with(admin()).contentType(MediaType.APPLICATION_JSON).content(TOPICO_VALIDO))
                .andExpect(status().isConflict());

        when(kafkaAdminService.modificarConfiguraciones(eq("t"), anyMap()))
                .thenThrow(new SolicitudInvalidaException("Configuracion no permitida: 'foo'"));
        mockMvc.perform(put("/api/topics/t/configs").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configs\":{\"foo\":\"1\"}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Configuracion no permitida: 'foo'"));

        mockMvc.perform(put("/api/topics/t/configs").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configs\":{}}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("5. PATCH particiones responde 200 y GET/DELETE de topico inexistente responde 404")
    void particionesYNoEncontrado() throws Exception {
        when(kafkaAdminService.aumentarParticiones("t", 6)).thenReturn(detalle("t", 6));
        mockMvc.perform(patch("/api/topics/t/partitions").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"partitions\":6}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitions").value(6));

        when(kafkaAdminService.describirTopico("nada")).thenThrow(new RecursoNoEncontradoException("No existe"));
        doThrow(new RecursoNoEncontradoException("No existe")).when(kafkaAdminService).eliminarTopico("nada");
        mockMvc.perform(get("/api/topics/nada").with(admin())).andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/topics/nada").with(admin())).andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("6. Topico protegido o grupo activo responden 409; eliminar responde 204")
    void operacionesNoPermitidas() throws Exception {
        doThrow(new OperacionNoPermitidaException("protegido")).when(kafkaAdminService).eliminarTopico("pedidos360.pedidos.eventos");
        mockMvc.perform(delete("/api/topics/pedidos360.pedidos.eventos").with(admin())).andExpect(status().isConflict());

        when(kafkaAdminService.reiniciarOffsets(eq("activo"), any())).thenThrow(new OperacionNoPermitidaException("activo"));
        mockMvc.perform(post("/api/consumer-groups/activo/reset-offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"t\",\"destino\":\"EARLIEST\"}"))
                .andExpect(status().isConflict());

        mockMvc.perform(delete("/api/topics/otro").with(admin())).andExpect(status().isNoContent());
        mockMvc.perform(delete("/api/consumer-groups/viejo").with(admin())).andExpect(status().isNoContent());
    }

    @Test
    @DisplayName("7. reset-offsets con destino invalido responde 400; listar grupos responde 200; cluster caido responde 503")
    void gruposYDisponibilidad() throws Exception {
        mockMvc.perform(post("/api/consumer-groups/g/reset-offsets").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"t\",\"destino\":\"AYER\"}"))
                .andExpect(status().isBadRequest());

        when(kafkaAdminService.listarGrupos()).thenReturn(List.of(new GrupoResumen("pedidos360-analitica", "STABLE")));
        mockMvc.perform(get("/api/consumer-groups").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].state").value("STABLE"));

        when(kafkaAdminService.describirCluster()).thenThrow(new KafkaNoDisponibleException("Kafka no respondio"));
        mockMvc.perform(get("/api/cluster").with(admin())).andExpect(status().isServiceUnavailable());
    }
}
