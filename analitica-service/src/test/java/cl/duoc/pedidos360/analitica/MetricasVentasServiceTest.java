package cl.duoc.pedidos360.analitica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cl.duoc.pedidos360.analitica.config.KafkaAnaliticaProperties;
import cl.duoc.pedidos360.analitica.evento.EventoInvalidoException;
import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka;
import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka.Item;
import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka.TipoEvento;
import cl.duoc.pedidos360.analitica.evento.ValidadorEventos;
import cl.duoc.pedidos360.analitica.metricas.MetricaVentasDia;
import cl.duoc.pedidos360.analitica.metricas.MetricasVentasService;
import jakarta.validation.Validation;

/**
 * Pruebas de la logica de dominio (sin Kafka): calculo de metricas, idempotencia,
 * cambios de estado y validacion de payloads.
 */
class MetricasVentasServiceTest {

    private static final LocalDateTime HOY = LocalDateTime.of(2026, 11, 15, 13, 0);

    private MetricasVentasService service;
    private ValidadorEventos validador;

    @BeforeEach
    void setUp() {
        var props = new KafkaAnaliticaProperties(null, null, null, new KafkaAnaliticaProperties.Idempotencia(100));
        service = new MetricasVentasService(props);
        validador = new ValidadorEventos(Validation.buildDefaultValidatorFactory().getValidator());
    }

    @Test
    @DisplayName("PEDIDO_CREADO suma pedidos, monto, ticket promedio y unidades por producto")
    void pedidosCreadosActualizanMetricas() {
        service.registrar(creado("PED-1", 10000.0, new Item("Pizza", 2, 4000.0, 8000.0), new Item("Bebida", 1, 2000.0, 2000.0)), 0);
        MetricaVentasDia metrica = service.registrar(creado("PED-2", 5000.0, new Item("Pizza", 1, 5000.0, 5000.0)), 0).metrica();

        assertThat(metrica.fecha()).isEqualTo(HOY.toLocalDate());
        assertThat(metrica.totalPedidos()).isEqualTo(2);
        assertThat(metrica.montoTotal()).isEqualTo(15000.0);
        assertThat(metrica.ticketPromedio()).isEqualTo(7500.0);
        assertThat(metrica.unidadesPorProducto()).isEqualTo(Map.of("Pizza", 3L, "Bebida", 1L));
        assertThat(metrica.productoMasVendido()).isEqualTo("Pizza");
        assertThat(metrica.pedidosPorEstado()).isEqualTo(Map.of("PENDIENTE", 2L));
    }

    @Test
    @DisplayName("Un evento repetido (mismo eventoId) no se cuenta dos veces")
    void eventoDuplicadoSeIgnora() {
        PedidoEventoKafka evento = creado("PED-1", 10000.0, new Item("Pizza", 1, 10000.0, 10000.0));
        assertThat(service.registrar(evento, 0).aplicado()).isTrue();

        MetricasVentasService.Resultado repetido = service.registrar(evento, 0);

        assertThat(repetido.aplicado()).isFalse();
        assertThat(repetido.metrica().totalPedidos()).isEqualTo(1);
        assertThat(repetido.metrica().montoTotal()).isEqualTo(10000.0);
    }

    @Test
    @DisplayName("ESTADO_ACTUALIZADO mueve el pedido de su estado anterior al nuevo")
    void cambioDeEstadoMueveElConteo() {
        service.registrar(creado("PED-1", 1000.0, new Item("Pizza", 1, 1000.0, 1000.0)), 0);
        service.registrar(creado("PED-2", 1000.0, new Item("Pizza", 1, 1000.0, 1000.0)), 0);

        MetricaVentasDia metrica = service.registrar(estado("PED-1", "EN_PREPARACION"), 0).metrica();

        assertThat(metrica.pedidosPorEstado()).isEqualTo(Map.of("PENDIENTE", 1L, "EN_PREPARACION", 1L));
        assertThat(metrica.totalPedidos()).isEqualTo(2);
    }

    @Test
    @DisplayName("Cada particion se calcula por separado y se reconstruye igual al releerla")
    void particionesIndependientesYReconstruibles() {
        PedidoEventoKafka enP0 = creado("PED-A", 1000.0, new Item("Pizza", 1, 1000.0, 1000.0));
        PedidoEventoKafka enP1 = creado("PED-B", 3000.0, new Item("Bebida", 3, 1000.0, 3000.0));

        MetricaVentasDia p0 = service.registrar(enP0, 0).metrica();
        MetricaVentasDia p1 = service.registrar(enP1, 1).metrica();
        assertThat(p0.clave()).isEqualTo("2026-11-15#p0");
        assertThat(p1.clave()).isEqualTo("2026-11-15#p1");
        assertThat(p0.totalPedidos()).isEqualTo(1);
        assertThat(p1.montoTotal()).isEqualTo(3000.0);

        // Rebalanceo: la particion 1 se descarta y se vuelve a leer desde el inicio
        service.descartar(List.of(1));
        MetricaVentasDia reconstruida = service.registrar(enP1, 1).metrica();

        assertThat(reconstruida).usingRecursiveComparison().isEqualTo(p1);
    }

    @Test
    @DisplayName("El validador rechaza payloads incompletos como no recuperables")
    void validadorRechazaPayloadsInvalidos() {
        var sinNumero = new PedidoEventoKafka(UUID.randomUUID().toString(), TipoEvento.PEDIDO_CREADO, 1L, " ",
                "Ana", "ana@duocuc.cl", "PENDIENTE", 1000.0, HOY, HOY, List.of(new Item("Pizza", 1, 1000.0, 1000.0)));
        var sinItems = new PedidoEventoKafka(UUID.randomUUID().toString(), TipoEvento.PEDIDO_CREADO, 1L, "PED-9",
                "Ana", "ana@duocuc.cl", "PENDIENTE", 1000.0, HOY, HOY, List.of());
        var cantidadNegativa = creado("PED-8", 1000.0, new Item("Pizza", -1, 1000.0, 1000.0));

        assertThatThrownBy(() -> validador.validar(sinNumero)).isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("numeroPedido");
        assertThatThrownBy(() -> validador.validar(sinItems)).isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("items");
        assertThatThrownBy(() -> validador.validar(cantidadNegativa)).isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("cantidad");
        assertThatThrownBy(() -> validador.validar(null)).isInstanceOf(EventoInvalidoException.class);
    }

    private static PedidoEventoKafka creado(String numero, double total, Item... items) {
        return new PedidoEventoKafka(UUID.randomUUID().toString(), TipoEvento.PEDIDO_CREADO, 1L, numero,
                "Ana", "ana@duocuc.cl", "PENDIENTE", total, HOY, HOY, List.of(items));
    }

    private static PedidoEventoKafka estado(String numero, String nuevoEstado) {
        return new PedidoEventoKafka(UUID.randomUUID().toString(), TipoEvento.ESTADO_ACTUALIZADO, 1L, numero,
                "Ana", "ana@duocuc.cl", nuevoEstado, 1000.0, HOY, HOY.plusMinutes(5), List.of());
    }
}
