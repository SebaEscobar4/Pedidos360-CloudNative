package cl.duoc.pedidos360.analitica.metricas;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * =========================================================================
 * DTO: MetricaVentasDia
 * =========================================================================
 * FUNCION: Foto PARCIAL de las metricas de ventas de un dia, calculada a partir de UNA particion
 *          del topico de pedidos. Se publica en 'pedidos360.metricas.ventas' con clave
 *          "fecha#pN" (ej: 2026-11-15#p1). El topico es compactado: Kafka guarda la ultima foto
 *          de cada clave, y el backend suma las fotos parciales de un dia para obtener el total.
 *
 *          Por que por particion: el consumer group reparte las particiones entre instancias
 *          (y hilos) de analitica-service. Cada una calcula SOLO lo que ve, sin pisar el
 *          resultado de las demas, asi el servicio escala horizontalmente sin perder exactitud.
 *
 * @param fecha               dia de creacion de los pedidos
 * @param particion           particion del topico de pedidos que origino esta foto
 * @param totalPedidos        pedidos creados ese dia
 * @param montoTotal          suma de los totales
 * @param ticketPromedio      montoTotal / totalPedidos
 * @param pedidosPorEstado    estado actual de los pedidos del dia (PENDIENTE, EN_PREPARACION...)
 * @param unidadesPorProducto unidades vendidas por producto
 * @param productoMasVendido  producto con mas unidades
 * @param eventosProcesados   eventos que actualizaron esta foto
 * @param actualizadoEn       momento del ultimo cambio
 */
public record MetricaVentasDia(
        LocalDate fecha,
        int particion,
        long totalPedidos,
        double montoTotal,
        double ticketPromedio,
        Map<String, Long> pedidosPorEstado,
        Map<String, Long> unidadesPorProducto,
        String productoMasVendido,
        long eventosProcesados,
        LocalDateTime actualizadoEn) {

    /** Clave del mensaje en Kafka: yyyy-MM-dd#pN. */
    public String clave() {
        return fecha + "#p" + particion;
    }
}
