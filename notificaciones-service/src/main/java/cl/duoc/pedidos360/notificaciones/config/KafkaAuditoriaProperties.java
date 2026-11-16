package cl.duoc.pedidos360.notificaciones.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * =========================================================================
 * CONFIGURACION: KafkaAuditoriaProperties
 * =========================================================================
 * FUNCION: Mapea el bloque 'app.kafka' de application.yml: activacion, nombres de topicos,
 *          concurrencia y politica de reintentos del consumidor de auditoria.
 * CONECTA CON: KafkaAuditoriaConfig y AuditoriaPedidoListener.
 */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaAuditoriaProperties(boolean enabled, Topicos topics, Consumo consumo, Reintentos reintentos) {

    public record Topicos(String pedidosEventos, String pedidosEventosDlt) {}

    public record Consumo(int concurrencia) {}

    public record Reintentos(int maximo, long intervaloInicialMs, double multiplicador) {}
}
