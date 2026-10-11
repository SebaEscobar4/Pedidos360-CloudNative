package cl.duoc.pedidos360.rabbitadmin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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

import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueResponse;
import cl.duoc.pedidos360.rabbitadmin.exception.RecursoNoEncontradoException;
import cl.duoc.pedidos360.rabbitadmin.exception.RecursoYaExisteException;
import cl.duoc.pedidos360.rabbitadmin.service.RabbitAdminService;

/**
 * Pruebas de la API de administracion: seguridad (401/403), validacion de entrada (400)
 * y codigos de respuesta (201/204/404/409). RabbitAdminService se simula, por lo que
 * no se necesita un broker RabbitMQ para ejecutarlas.
 */
@SpringBootTest
@AutoConfigureMockMvc
class RabbitAdminApiTests {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RabbitAdminService rabbitAdminService;

    // Evita consultar las llaves publicas de Azure AD durante las pruebas
    @MockitoBean
    private JwtDecoder jwtDecoder;

    private static RequestPostProcessor admin() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    @Test
    @DisplayName("1. Sin token responde 401")
    void sinToken_401() throws Exception {
        mockMvc.perform(post("/api/queues").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"demo\"}"))
                .andExpect(status().isUnauthorized());
        verifyNoInteractions(rabbitAdminService);
    }

    @Test
    @DisplayName("2. Token valido sin rol ADMIN responde 403")
    void sinRolAdmin_403() throws Exception {
        mockMvc.perform(post("/api/queues").with(jwt())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"demo\"}"))
                .andExpect(status().isForbidden());
        verifyNoInteractions(rabbitAdminService);
    }

    @Test
    @DisplayName("3. POST /api/queues con datos validos responde 201")
    void crearCola_201() throws Exception {
        when(rabbitAdminService.crearCola(any())).thenReturn(new QueueResponse("demo.queue", 0, 0));
        mockMvc.perform(post("/api/queues").with(admin())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo.queue\",\"deadLetterExchange\":\"pedidos360.dlx\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("demo.queue"));
    }

    @Test
    @DisplayName("4. POST /api/queues con nombre vacio responde 400 y no llama al servicio")
    void crearCola_nombreVacio_400() throws Exception {
        mockMvc.perform(post("/api/queues").with(admin())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"   \"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details").isNotEmpty());
        verifyNoInteractions(rabbitAdminService);
    }

    @Test
    @DisplayName("5. POST /api/queues con configuracion invalida responde 400")
    void crearCola_configuracionInvalida_400() throws Exception {
        // TTL negativo, nombre reservado 'amq.' y routing key de DLQ sin exchange de DLQ
        mockMvc.perform(post("/api/queues").with(admin())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"amq.demo\",\"messageTtl\":-5,\"deadLetterRoutingKey\":\"x\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details.length()").value(3));
        verifyNoInteractions(rabbitAdminService);
    }

    @Test
    @DisplayName("6. POST /api/queues duplicada responde 409")
    void crearCola_duplicada_409() throws Exception {
        when(rabbitAdminService.crearCola(any())).thenThrow(new RecursoYaExisteException("Ya existe"));
        mockMvc.perform(post("/api/queues").with(admin())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"demo.queue\"}"))
                .andExpect(status().isConflict());
    }

    @Test
    @DisplayName("7. DELETE /api/queues/{name} responde 204 y 404 si no existe")
    void eliminarCola_204_y_404() throws Exception {
        mockMvc.perform(delete("/api/queues/demo.queue").with(admin()))
                .andExpect(status().isNoContent());
        verify(rabbitAdminService).eliminarCola("demo.queue");

        doThrow(new RecursoNoEncontradoException("No existe")).when(rabbitAdminService).eliminarCola("otra");
        mockMvc.perform(delete("/api/queues/otra").with(admin()))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("8. POST /api/exchanges con tipo invalido responde 400")
    void crearExchange_tipoInvalido_400() throws Exception {
        mockMvc.perform(post("/api/exchanges").with(admin())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo.exchange\",\"type\":\"inventado\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(rabbitAdminService);
    }

    @Test
    @DisplayName("9. POST /api/bindings sin cola responde 400")
    void crearBinding_sinCola_400() throws Exception {
        mockMvc.perform(post("/api/bindings").with(admin())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"exchange\":\"demo.exchange\",\"routingKey\":\"demo\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(rabbitAdminService);
    }

    @Test
    @DisplayName("10. DELETE /api/bindings valida parametros y responde 204")
    void eliminarBinding_204_y_400() throws Exception {
        mockMvc.perform(delete("/api/bindings").with(admin())
                        .param("queue", "demo.queue").param("exchange", "demo.exchange").param("routingKey", "demo"))
                .andExpect(status().isNoContent());
        verify(rabbitAdminService).eliminarBinding(new BindingRequest("demo.queue", "demo.exchange", "demo"));

        mockMvc.perform(delete("/api/bindings").with(admin()).param("queue", "demo.queue"))
                .andExpect(status().isBadRequest());
    }
}
