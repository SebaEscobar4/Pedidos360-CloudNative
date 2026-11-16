package cl.duoc.pedidos360.kafkaadmin;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * =========================================================================
 * PRUEBA DE INTEGRACION: KafkaAdminIntegrationTest
 * =========================================================================
 * FUNCION: Recorre la API completa contra el cluster real de docker-compose.kafka.yml:
 *          cluster, ciclo de vida de un topico (crear, configurar, restablecer, ampliar, eliminar),
 *          inspeccion de consumer groups con lag, reinicio de offsets y protecciones (409).
 *          Cada llamada queda registrada en target/kafka-admin-demo.txt (metodo, ruta, codigo y respuesta).
 *
 *   docker compose -f docker-compose.kafka.yml up -d
 *   KAFKA_IT=true ./mvnw test -Dtest=KafkaAdminIntegrationTest
 *   (KAFKA_IT_CONSERVAR=true deja el topico de prueba creado para verlo en Kafka-UI)
 */
@SpringBootTest
@AutoConfigureMockMvc
@EnabledIfEnvironmentVariable(named = "KAFKA_IT", matches = "true")
class KafkaAdminIntegrationTest {

    private static final String TOPICO = "pedidos360.reportes.diarios";
    private static final String GRUPO_DEMO = "pedidos360-reportes";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    private final List<String> registro = new ArrayList<>();

    @Test
    @DisplayName("Administracion completa de topicos y consumer groups contra el cluster real")
    void administracionCompleta() throws Exception {
        // Deja el ambiente limpio si una ejecucion anterior conservo el topico
        mockMvc.perform(conAdmin(delete("/api/topics/" + TOPICO)));

        llamar("Sin token: la API exige JWT", get("/api/topics"), false).andExpect(status().isUnauthorized());
        llamar("Estado del cluster", get("/api/cluster")).andExpect(status().isOk())
                .andExpect(jsonPath("$.brokers.length()").value(3));

        llamar("Crear topico con configuracion NO permitida", post("/api/topics").contentType(MediaType.APPLICATION_JSON)
                .content(json(TOPICO, "\"unclean.leader.election.enable\": \"true\"")))
                .andExpect(status().isBadRequest());
        llamar("Crear topico con mas replicas que brokers", post("/api/topics").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"" + TOPICO + "\",\"partitions\":3,\"replicationFactor\":5}"))
                .andExpect(status().isBadRequest());
        llamar("Crear topico con particiones, replicas y configuraciones", post("/api/topics").contentType(MediaType.APPLICATION_JSON)
                .content(json(TOPICO, "\"cleanup.policy\": \"delete\", \"retention.ms\": \"86400000\", \"min.insync.replicas\": \"2\"")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.partitions").value(3))
                .andExpect(jsonPath("$.replicationFactor").value(3))
                .andExpect(jsonPath("$.configuracionPersonalizada['retention.ms']").value("86400000"));
        llamar("Crear el mismo topico otra vez", post("/api/topics").contentType(MediaType.APPLICATION_JSON)
                .content(json(TOPICO, "\"cleanup.policy\": \"delete\"")))
                .andExpect(status().isConflict());

        llamar("Modificar retencion (3 dias) y compresion", put("/api/topics/" + TOPICO + "/configs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"configs\":{\"retention.ms\":\"259200000\",\"compression.type\":\"lz4\"}}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configuracionPersonalizada['retention.ms']").value("259200000"))
                .andExpect(jsonPath("$.configuracionPersonalizada['compression.type']").value("lz4"));
        llamar("min.insync.replicas mayor que el factor de replicacion", put("/api/topics/" + TOPICO + "/configs")
                .contentType(MediaType.APPLICATION_JSON).content("{\"configs\":{\"min.insync.replicas\":\"5\"}}"))
                .andExpect(status().isBadRequest());
        llamar("Restablecer compression.type al valor por defecto", delete("/api/topics/" + TOPICO + "/configs/compression.type"))
                .andExpect(status().isNoContent());

        llamar("Aumentar particiones de 3 a 6", patch("/api/topics/" + TOPICO + "/partitions").contentType(MediaType.APPLICATION_JSON)
                .content("{\"partitions\":6}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.particiones.length()").value(6));
        llamar("Intentar reducir particiones", patch("/api/topics/" + TOPICO + "/partitions").contentType(MediaType.APPLICATION_JSON)
                .content("{\"partitions\":2}"))
                .andExpect(status().isBadRequest());
        llamar("Detalle del topico (lider, replicas, ISR y configuracion)", get("/api/topics/" + TOPICO))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configuracionPersonalizada['compression.type']").doesNotExist());

        llamar("Listar consumer groups", get("/api/consumer-groups")).andExpect(status().isOk());
        llamar("Inspeccionar el grupo de analitica (miembros, offsets y lag)", get("/api/consumer-groups/pedidos360-analitica"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("STABLE"));
        llamar("Reiniciar offsets de un grupo con consumidores activos", post("/api/consumer-groups/pedidos360-analitica/reset-offsets")
                .contentType(MediaType.APPLICATION_JSON).content("{\"topic\":\"pedidos360.pedidos.eventos\",\"destino\":\"EARLIEST\"}"))
                .andExpect(status().isConflict());
        llamar("Reiniciar offsets de un grupo detenido (lo deja listo para leer desde el inicio)",
                post("/api/consumer-groups/" + GRUPO_DEMO + "/reset-offsets").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"pedidos360.pedidos.eventos\",\"destino\":\"EARLIEST\"}"))
                .andExpect(status().isOk());
        llamar("Inspeccionar el grupo reiniciado (lag = eventos por leer)", get("/api/consumer-groups/" + GRUPO_DEMO))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("EMPTY"));
        llamar("Eliminar el grupo", delete("/api/consumer-groups/" + GRUPO_DEMO)).andExpect(status().isNoContent());
        llamar("Consultar el grupo eliminado", get("/api/consumer-groups/" + GRUPO_DEMO)).andExpect(status().isNotFound());

        llamar("Eliminar un topico protegido", delete("/api/topics/pedidos360.pedidos.eventos")).andExpect(status().isConflict());
        if (!"true".equals(System.getenv("KAFKA_IT_CONSERVAR"))) {
            llamar("Eliminar el topico de prueba", delete("/api/topics/" + TOPICO)).andExpect(status().isNoContent());
            Thread.sleep(1500);
            llamar("Consultar el topico eliminado", get("/api/topics/" + TOPICO)).andExpect(status().isNotFound());
        }

        Files.write(Path.of("target", "kafka-admin-demo.txt"), registro, StandardCharsets.UTF_8);
    }

    private ResultActions llamar(String paso, MockHttpServletRequestBuilder request) throws Exception {
        return llamar(paso, request, true);
    }

    private ResultActions llamar(String paso, MockHttpServletRequestBuilder request, boolean autenticado) throws Exception {
        ResultActions resultado = mockMvc.perform(autenticado ? conAdmin(request) : request);
        var mvc = resultado.andReturn();
        String cuerpo = mvc.getResponse().getContentAsString();
        String peticion = mvc.getRequest().getContentAsString();
        registro.add("### " + (registro.stream().filter(l -> l.startsWith("### ")).count() + 1) + ". " + paso);
        registro.add(mvc.getRequest().getMethod() + " " + mvc.getRequest().getRequestURI()
                + (peticion == null || peticion.isBlank() ? "" : "  " + peticion.replaceAll("\\s+", " ")));
        registro.add("-> " + mvc.getResponse().getStatus() + (cuerpo.isBlank() ? "" : "  " + resumir(cuerpo)));
        registro.add("");
        return resultado;
    }

    private String resumir(String cuerpo) {
        try {
            String bonito = objectMapper.copy().enable(SerializationFeature.INDENT_OUTPUT)
                    .writeValueAsString(objectMapper.readTree(cuerpo));
            return bonito.length() > 1500 ? bonito.substring(0, 1500) + "\n   ..." : bonito;
        } catch (Exception e) {
            return cuerpo;
        }
    }

    private static MockHttpServletRequestBuilder conAdmin(MockHttpServletRequestBuilder request) {
        return request.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN")));
    }

    private static String json(String nombre, String configs) {
        return "{\"name\":\"" + nombre + "\",\"partitions\":3,\"replicationFactor\":3,\"configs\":{" + configs + "}}";
    }
}
