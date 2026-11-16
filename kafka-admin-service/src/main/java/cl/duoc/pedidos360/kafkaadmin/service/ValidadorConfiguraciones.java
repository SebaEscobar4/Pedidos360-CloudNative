package cl.duoc.pedidos360.kafkaadmin.service;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.kafkaadmin.exception.SolicitudInvalidaException;

/**
 * =========================================================================
 * VALIDACION: ValidadorConfiguraciones
 * =========================================================================
 * FUNCION: Lista blanca de configuraciones de topico que se pueden administrar desde la API,
 *          con la regla de valor de cada una. Evita enviar a Kafka parametros desconocidos o
 *          valores sin sentido y entrega un mensaje claro (400) antes de tocar el cluster.
 *
 *   cleanup.policy            delete | compact | compact,delete
 *   retention.ms / bytes      -1 (sin limite) o un numero positivo
 *   min.insync.replicas       entero >= 1 y <= factor de replicacion
 *   segment.ms / segment.bytes, delete.retention.ms, min/max.compaction.lag.ms, max.message.bytes
 *   min.cleanable.dirty.ratio entre 0 y 1
 *   compression.type          producer | uncompressed | gzip | snappy | lz4 | zstd
 *   message.timestamp.type    CreateTime | LogAppendTime
 * CONECTA CON: KafkaAdminService.
 */
@Component
public class ValidadorConfiguraciones {

    private static final Map<String, Regla> REGLAS = Map.ofEntries(
            Map.entry("cleanup.policy", new Regla(
                    v -> Set.of("delete", "compact", "compact,delete", "delete,compact").contains(v.replace(" ", "")),
                    "delete, compact o compact,delete")),
            Map.entry("retention.ms", new Regla(v -> esLargo(v, -1), "-1 (sin limite) o un numero de milisegundos")),
            Map.entry("retention.bytes", new Regla(v -> esLargo(v, -1), "-1 (sin limite) o un numero de bytes")),
            Map.entry("min.insync.replicas", new Regla(v -> esLargo(v, 1), "un entero mayor o igual a 1")),
            Map.entry("segment.ms", new Regla(v -> esLargo(v, 1), "un numero de milisegundos mayor a 0")),
            Map.entry("segment.bytes", new Regla(v -> esLargo(v, 1_048_576), "un numero de bytes de al menos 1048576")),
            Map.entry("delete.retention.ms", new Regla(v -> esLargo(v, 0), "un numero de milisegundos mayor o igual a 0")),
            Map.entry("min.compaction.lag.ms", new Regla(v -> esLargo(v, 0), "un numero de milisegundos mayor o igual a 0")),
            Map.entry("max.compaction.lag.ms", new Regla(v -> esLargo(v, 1), "un numero de milisegundos mayor a 0")),
            Map.entry("max.message.bytes", new Regla(v -> esLargo(v, 0), "un numero de bytes mayor o igual a 0")),
            Map.entry("min.cleanable.dirty.ratio", new Regla(ValidadorConfiguraciones::esRatio, "un decimal entre 0 y 1")),
            Map.entry("compression.type", new Regla(
                    v -> Set.of("producer", "uncompressed", "gzip", "snappy", "lz4", "zstd").contains(v),
                    "producer, uncompressed, gzip, snappy, lz4 o zstd")),
            Map.entry("message.timestamp.type", new Regla(
                    v -> Set.of("CreateTime", "LogAppendTime").contains(v), "CreateTime o LogAppendTime")));

    private record Regla(Predicate<String> valida, String esperado) {}

    /** Configuraciones que la API permite administrar (para mensajes de error y documentacion). */
    public Set<String> permitidas() {
        return new TreeSet<>(REGLAS.keySet());
    }

    /**
     * @param configs            configuraciones a validar
     * @param factorReplicacion  factor del topico (para acotar min.insync.replicas), o null si no se conoce
     */
    public void validar(Map<String, String> configs, Integer factorReplicacion) {
        configs.forEach((clave, valor) -> {
            Regla regla = REGLAS.get(clave);
            if (regla == null) {
                throw new SolicitudInvalidaException("Configuracion no permitida: '" + clave
                        + "'. Se pueden administrar: " + permitidas());
            }
            if (valor == null || valor.isBlank() || !regla.valida().test(valor.trim())) {
                throw new SolicitudInvalidaException("Valor invalido para '" + clave + "': '" + valor
                        + "'. Se espera " + regla.esperado());
            }
        });

        String minIsr = configs.get("min.insync.replicas");
        if (minIsr != null && factorReplicacion != null && Long.parseLong(minIsr.trim()) > factorReplicacion) {
            throw new SolicitudInvalidaException("min.insync.replicas (" + minIsr.trim()
                    + ") no puede ser mayor que el factor de replicacion (" + factorReplicacion + ")");
        }
    }

    private static boolean esLargo(String valor, long minimo) {
        try {
            return Long.parseLong(valor) >= minimo;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean esRatio(String valor) {
        try {
            double d = Double.parseDouble(valor);
            return d >= 0 && d <= 1;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
