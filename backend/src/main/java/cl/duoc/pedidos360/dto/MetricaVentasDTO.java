package cl.duoc.pedidos360.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * =========================================================================
 * DTO: MetricaVentasDTO
 * =========================================================================
 * FUNCION: Copia local del contrato JSON que analitica-service publica en
 *          'pedidos360.metricas.ventas': la foto de las ventas de un dia.
 *          En Kafka cada mensaje es una foto PARCIAL (una por particion del topico de pedidos,
 *          clave "fecha#pN"); MetricasVentasStore las suma para entregar el total del dia,
 *          que se informa con particion = null.
 * CONECTA CON: MetricasVentasListener (Kafka), MetricasVentasStore y AdminMetricasController.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricaVentasDTO(
        LocalDate fecha,
        Integer particion,
        long totalPedidos,
        double montoTotal,
        double ticketPromedio,
        Map<String, Long> pedidosPorEstado,
        Map<String, Long> unidadesPorProducto,
        String productoMasVendido,
        long eventosProcesados,
        LocalDateTime actualizadoEn) {}
