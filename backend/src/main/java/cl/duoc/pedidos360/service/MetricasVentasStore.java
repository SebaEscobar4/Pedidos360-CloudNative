package cl.duoc.pedidos360.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.dto.MetricaVentasDTO;

/**
 * =========================================================================
 * SERVICIO: MetricasVentasStore
 * =========================================================================
 * FUNCION: Vista en memoria de las metricas de ventas. Guarda la ultima foto PARCIAL de cada
 *          clave de Kafka ("fecha#pN") y, al consultar, suma las fotos de un mismo dia.
 *          No conoce Kafka.
 * CONECTA CON: MetricasVentasListener (escribe) y AdminMetricasController (lee).
 */
@Component
public class MetricasVentasStore {

    private final Map<String, MetricaVentasDTO> porClave = new ConcurrentHashMap<>();

    public void guardar(String clave, MetricaVentasDTO parcial) {
        porClave.put(clave, parcial);
    }

    public void eliminar(String clave) {
        porClave.remove(clave);
    }

    /** Metricas totales de cada dia, la mas reciente primero. */
    public List<MetricaVentasDTO> listar() {
        return porClave.values().stream()
                .collect(Collectors.groupingBy(MetricaVentasDTO::fecha))
                .entrySet().stream()
                .map(dia -> combinar(dia.getKey(), dia.getValue()))
                .sorted(Comparator.comparing(MetricaVentasDTO::fecha).reversed())
                .toList();
    }

    public Optional<MetricaVentasDTO> buscar(LocalDate fecha) {
        List<MetricaVentasDTO> parciales = porClave.values().stream()
                .filter(m -> fecha.equals(m.fecha()))
                .toList();
        return parciales.isEmpty() ? Optional.empty() : Optional.of(combinar(fecha, parciales));
    }

    private static MetricaVentasDTO combinar(LocalDate fecha, List<MetricaVentasDTO> parciales) {
        long pedidos = parciales.stream().mapToLong(MetricaVentasDTO::totalPedidos).sum();
        double monto = parciales.stream().mapToDouble(MetricaVentasDTO::montoTotal).sum();
        long eventos = parciales.stream().mapToLong(MetricaVentasDTO::eventosProcesados).sum();
        Map<String, Long> estados = sumar(parciales.stream().map(MetricaVentasDTO::pedidosPorEstado).toList());
        Map<String, Long> productos = sumar(parciales.stream().map(MetricaVentasDTO::unidadesPorProducto).toList());
        String masVendido = productos.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
        LocalDateTime actualizado = parciales.stream()
                .map(MetricaVentasDTO::actualizadoEn)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElse(null);
        double promedio = pedidos == 0 ? 0 : Math.round(monto / pedidos * 100.0) / 100.0;
        return new MetricaVentasDTO(fecha, null, pedidos, monto, promedio, estados, productos, masVendido, eventos, actualizado);
    }

    private static Map<String, Long> sumar(List<Map<String, Long>> mapas) {
        Map<String, Long> total = new TreeMap<>();
        mapas.stream().filter(Objects::nonNull).forEach(m -> m.forEach((k, v) -> total.merge(k, v, Long::sum)));
        total.values().removeIf(v -> v == 0);
        return total;
    }
}
