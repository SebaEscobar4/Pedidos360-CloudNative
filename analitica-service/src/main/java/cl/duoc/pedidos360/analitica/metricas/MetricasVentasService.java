package cl.duoc.pedidos360.analitica.metricas;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.analitica.config.KafkaAnaliticaProperties;
import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka;

/**
 * =========================================================================
 * SERVICIO DE DOMINIO: MetricasVentasService
 * =========================================================================
 * FUNCION: Calcula en tiempo real las metricas de ventas por dia a partir del flujo de eventos:
 *            PEDIDO_CREADO      -> suma el pedido, su monto y las unidades de cada producto.
 *            ESTADO_ACTUALIZADO -> mueve el pedido de su estado anterior al nuevo.
 *
 *  - Estado separado POR PARTICION: todos los eventos de un pedido llegan a la misma particion
 *    (clave = numero de pedido), asi que cada particion se puede calcular de forma independiente.
 *    Si la particion pasa a otra instancia (rebalanceo), esta descarta su estado y la nueva
 *    la reconstruye leyendola desde el inicio.
 *  - Idempotente: cada eventoId se procesa una sola vez (Kafka entrega "al menos una vez").
 *  - No conoce Kafka: recibe DTO ya validados y devuelve la foto actualizada.
 *
 * CONECTA CON: PedidoEventosListener.
 */
@Service
public class MetricasVentasService {

    private static final Logger log = LoggerFactory.getLogger(MetricasVentasService.class);

    /** Resultado de registrar un evento: si cambio algo y la foto del dia afectado. */
    public record Resultado(boolean aplicado, MetricaVentasDia metrica) {}

    private final int capacidadIdempotencia;
    // Cada particion la procesa un unico hilo a la vez, pero distintos hilos tocan el mapa
    private final Map<Integer, EstadoParticion> particiones = new ConcurrentHashMap<>();

    public MetricasVentasService(KafkaAnaliticaProperties props) {
        this.capacidadIdempotencia = props.idempotencia().capacidad();
    }

    public Resultado registrar(PedidoEventoKafka evento, int particion) {
        EstadoParticion estado = particiones.computeIfAbsent(particion, p -> new EstadoParticion(capacidadIdempotencia));
        synchronized (estado) {
            return estado.registrar(evento, particion);
        }
    }

    /** La particion dejo de pertenecer a esta instancia (o se va a reconstruir): se olvida su estado. */
    public void descartar(Collection<Integer> idsParticion) {
        idsParticion.forEach(particiones::remove);
        if (!idsParticion.isEmpty()) {
            log.info("[ANALITICA] Estado descartado de las particiones {}", idsParticion);
        }
    }

    // ------------------------------------------------------------------ estado interno

    private record PedidoConocido(LocalDate dia, String estado) {}

    private static final class EstadoParticion {
        private final Map<LocalDate, Acumulado> porDia = new HashMap<>();
        private final Map<String, PedidoConocido> pedidos = new HashMap<>();
        private final Set<String> eventosProcesados;

        EstadoParticion(int capacidad) {
            // Conjunto acotado: recuerda los ultimos N eventoId (los mas antiguos se descartan)
            this.eventosProcesados = Collections.newSetFromMap(new LinkedHashMap<>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > capacidad;
                }
            });
        }

        Resultado registrar(PedidoEventoKafka evento, int particion) {
            PedidoConocido conocido = pedidos.get(evento.numeroPedido());
            LocalDate dia = conocido != null ? conocido.dia() : diaDelEvento(evento);
            Acumulado acumulado = porDia.computeIfAbsent(dia, d -> new Acumulado());

            if (!eventosProcesados.add(evento.eventoId())) {
                log.info("[ANALITICA] Evento duplicado ignorado. eventoId={} pedido={}", evento.eventoId(), evento.numeroPedido());
                return new Resultado(false, acumulado.foto(dia, particion));
            }

            switch (evento.tipo()) {
                case PEDIDO_CREADO -> {
                    if (conocido != null) {
                        log.info("[ANALITICA] El pedido {} ya estaba contabilizado", evento.numeroPedido());
                        return new Resultado(false, acumulado.foto(dia, particion));
                    }
                    acumulado.totalPedidos++;
                    acumulado.montoTotal += evento.total();
                    evento.items().forEach(item ->
                            acumulado.unidadesPorProducto.merge(item.productoNombre(), (long) item.cantidad(), Long::sum));
                    acumulado.sumarEstado(evento.estado(), 1);
                }
                case ESTADO_ACTUALIZADO -> {
                    if (conocido != null) {
                        acumulado.sumarEstado(conocido.estado(), -1);
                    }
                    // Pedido creado antes de la retencion del topico: solo se registra su estado actual
                    acumulado.sumarEstado(evento.estado(), 1);
                }
            }

            pedidos.put(evento.numeroPedido(), new PedidoConocido(dia, evento.estado()));
            acumulado.eventos++;
            acumulado.actualizadoEn = evento.fechaEvento();
            return new Resultado(true, acumulado.foto(dia, particion));
        }

        private static LocalDate diaDelEvento(PedidoEventoKafka evento) {
            return (evento.fechaCreacion() != null ? evento.fechaCreacion() : evento.fechaEvento()).toLocalDate();
        }
    }

    private static final class Acumulado {
        long totalPedidos;
        double montoTotal;
        long eventos;
        LocalDateTime actualizadoEn;
        final Map<String, Long> pedidosPorEstado = new TreeMap<>();
        final Map<String, Long> unidadesPorProducto = new TreeMap<>();

        void sumarEstado(String estado, long delta) {
            pedidosPorEstado.merge(estado, delta, Long::sum);
            pedidosPorEstado.remove(estado, 0L);
        }

        MetricaVentasDia foto(LocalDate dia, int particion) {
            String masVendido = unidadesPorProducto.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);
            double promedio = totalPedidos == 0 ? 0 : Math.round(montoTotal / totalPedidos * 100.0) / 100.0;
            return new MetricaVentasDia(dia, particion, totalPedidos, montoTotal, promedio,
                    Map.copyOf(pedidosPorEstado), Map.copyOf(unidadesPorProducto),
                    masVendido, eventos, actualizadoEn);
        }
    }
}
