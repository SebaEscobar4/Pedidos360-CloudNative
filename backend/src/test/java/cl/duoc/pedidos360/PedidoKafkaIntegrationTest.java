package cl.duoc.pedidos360;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * =========================================================================
 * PRUEBA DE INTEGRACION: PedidoKafkaIntegrationTest
 * =========================================================================
 * FUNCION: Comprueba contra el cluster real (docker-compose.kafka.yml) que crear un pedido
 *          y cambiar su estado publican dos eventos en 'pedidos360.pedidos.eventos':
 *            - con el numero de pedido como clave (misma particion -> orden garantizado),
 *            - con las cabeceras 'tipo-evento' y 'origen',
 *            - con el DTO PedidoEvento en JSON (eventoId, fechas ISO-8601, items).
 *
 * Solo se ejecuta con la variable KAFKA_IT=true:
 *   docker compose -f docker-compose.kafka.yml up -d
 *   KAFKA_IT=true ./mvnw test -Dtest=PedidoKafkaIntegrationTest
 */
@SpringBootTest(properties = "app.kafka.enabled=true")
@AutoConfigureMockMvc
@EnabledIfEnvironmentVariable(named = "KAFKA_IT", matches = "true")
class PedidoKafkaIntegrationTest {

    private static final String TOPICO = "pedidos360.pedidos.eventos";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    // Evita consultar las llaves publicas de Azure AD durante la prueba
    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    @DisplayName("Crear un pedido y cambiar su estado publica dos eventos ordenados en Kafka")
    void pedidoPublicaEventosEnKafka() throws Exception {
        String respuesta = mockMvc.perform(post("/api/pedidos")
                        .with(jwt().authorities(new SimpleGrantedAuthority("SCOPE_recurso.write")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "clienteNombre": "Prueba Kafka",
                                  "clienteEmail": "kafka@duocuc.cl",
                                  "items": [ { "productoId": 1, "cantidad": 2 }, { "productoId": 3, "cantidad": 1 } ]
                                }
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        JsonNode pedido = objectMapper.readTree(respuesta);
        String numeroPedido = pedido.get("numeroPedido").asText();

        mockMvc.perform(patch("/api/admin/pedidos/" + pedido.get("id").asLong() + "/estado")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"estado\":\"EN_PREPARACION\"}"))
                .andExpect(status().isOk());

        List<ConsumerRecord<String, String>> eventos = leerEventosDelPedido(numeroPedido, 2);

        assertThat(eventos).hasSize(2);
        assertThat(eventos).extracting(ConsumerRecord::partition).containsOnly(eventos.get(0).partition());
        assertThat(eventos).extracting(r -> cabecera(r, "tipo-evento"))
                .containsExactly("PEDIDO_CREADO", "ESTADO_ACTUALIZADO");
        assertThat(cabecera(eventos.get(0), "origen")).isEqualTo("pedidos360-backend");

        JsonNode creado = objectMapper.readTree(eventos.get(0).value());
        assertThat(creado.get("eventoId").asText()).isNotBlank();
        assertThat(creado.get("clienteEmail").asText()).isEqualTo("kafka@duocuc.cl");
        assertThat(creado.get("items")).hasSize(2);
        assertThat(creado.get("fechaCreacion").isTextual()).isTrue();
        assertThat(objectMapper.readTree(eventos.get(1).value()).get("estado").asText()).isEqualTo("EN_PREPARACION");
    }

    /**
     * Circuito completo (requiere ademas analitica-service en ejecucion):
     * backend -> pedidos.eventos -> analitica-service -> metricas.ventas -> backend -> GET /api/admin/metricas
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_IT_ANALITICA", matches = "true")
    @DisplayName("Las metricas calculadas por analitica-service vuelven al backend por Kafka")
    void metricasVuelvenAlBackend() throws Exception {
        String hoy = java.time.LocalDate.now().toString();
        long antes = pedidosDeHoy(hoy);

        mockMvc.perform(post("/api/pedidos")
                        .with(jwt().authorities(new SimpleGrantedAuthority("SCOPE_recurso.write")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                { "clienteNombre": "Circuito Kafka", "clienteEmail": "circuito@duocuc.cl",
                                  "items": [ { "productoId": 2, "cantidad": 1 } ] }
                                """))
                .andExpect(status().isCreated());

        long limite = System.currentTimeMillis() + 30_000;
        long despues = antes;
        while (despues <= antes && System.currentTimeMillis() < limite) {
            Thread.sleep(500);
            despues = pedidosDeHoy(hoy);
        }
        assertThat(despues).isGreaterThan(antes);
    }

    private long pedidosDeHoy(String hoy) throws Exception {
        String json = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .get("/api/admin/metricas")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"))))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        for (JsonNode metrica : objectMapper.readTree(json)) {
            if (hoy.equals(metrica.get("fecha").asText())) {
                return metrica.get("totalPedidos").asLong();
            }
        }
        return 0;
    }

    private List<ConsumerRecord<String, String>> leerEventosDelPedido(String numeroPedido, int esperados) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> encontrados = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            // Lectura sin consumer group (assign): la prueba no deja grupos huerfanos en el cluster
            List<org.apache.kafka.common.TopicPartition> particiones = consumer.partitionsFor(TOPICO).stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(TOPICO, p.partition()))
                    .toList();
            consumer.assign(particiones);
            consumer.seekToBeginning(particiones);
            long limite = System.currentTimeMillis() + 20_000;
            while (encontrados.size() < esperados && System.currentTimeMillis() < limite) {
                consumer.poll(Duration.ofMillis(500)).forEach(registro -> {
                    if (numeroPedido.equals(registro.key())) {
                        encontrados.add(registro);
                    }
                });
            }
        }
        return encontrados;
    }

    private static String cabecera(ConsumerRecord<String, String> registro, String nombre) {
        var header = registro.headers().lastHeader(nombre);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
