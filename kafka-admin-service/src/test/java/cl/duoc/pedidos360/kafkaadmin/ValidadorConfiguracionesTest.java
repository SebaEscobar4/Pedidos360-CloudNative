package cl.duoc.pedidos360.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cl.duoc.pedidos360.kafkaadmin.exception.SolicitudInvalidaException;
import cl.duoc.pedidos360.kafkaadmin.service.ValidadorConfiguraciones;

/**
 * Reglas de las configuraciones de topico que acepta la API.
 */
class ValidadorConfiguracionesTest {

    private final ValidadorConfiguraciones validador = new ValidadorConfiguraciones();

    @Test
    @DisplayName("Acepta configuraciones validas de retencion, compactacion y replicas")
    void aceptaValidas() {
        assertThatCode(() -> validador.validar(Map.of(
                "cleanup.policy", "compact,delete",
                "retention.ms", "-1",
                "min.insync.replicas", "2",
                "min.cleanable.dirty.ratio", "0.1",
                "compression.type", "lz4"), 3)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Rechaza claves desconocidas, valores invalidos y min.insync.replicas mayor al factor")
    void rechazaInvalidas() {
        assertThatThrownBy(() -> validador.validar(Map.of("unclean.leader.election.enable", "true"), 3))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("no permitida");
        assertThatThrownBy(() -> validador.validar(Map.of("cleanup.policy", "borrar"), 3))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("cleanup.policy");
        assertThatThrownBy(() -> validador.validar(Map.of("retention.ms", "siete dias"), 3))
                .isInstanceOf(SolicitudInvalidaException.class);
        assertThatThrownBy(() -> validador.validar(Map.of("min.cleanable.dirty.ratio", "1.5"), 3))
                .isInstanceOf(SolicitudInvalidaException.class);
        assertThatThrownBy(() -> validador.validar(Map.of("min.insync.replicas", "3"), 2))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("factor de replicacion");
    }
}
