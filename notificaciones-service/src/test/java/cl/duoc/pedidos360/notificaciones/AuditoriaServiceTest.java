package cl.duoc.pedidos360.notificaciones;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import cl.duoc.pedidos360.notificaciones.auditoria.AuditoriaService;
import cl.duoc.pedidos360.notificaciones.messaging.ErrorNoRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.ErrorRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * Pruebas del dominio 'auditoria' (sin Kafka): registro en archivo, idempotencia,
 * payload invalido (no recuperable) y fallo de escritura (recuperable).
 */
class AuditoriaServiceTest {

    private static final LocalDateTime FECHA = LocalDateTime.of(2026, 11, 15, 19, 30);

    @TempDir
    Path tmp;

    @Test
    @DisplayName("Registra una linea por evento y no duplica un eventoId repetido")
    void registraUnaVezCadaEvento() throws Exception {
        AuditoriaService service = new AuditoriaService(tmp.toString(), 100);
        PedidoEvento evento = evento("evt-1", "PED-1");

        assertThat(service.registrar(evento, "pedidos360.pedidos.eventos-0@5")).isTrue();
        assertThat(service.registrar(evento, "pedidos360.pedidos.eventos-0@5")).isFalse();
        service.registrar(evento("evt-2", "PED-2"), "pedidos360.pedidos.eventos-1@0");

        List<String> lineas = Files.readAllLines(tmp.resolve("pedidos-2026-11-15.txt"));
        assertThat(lineas).hasSize(2);
        assertThat(lineas.get(0)).contains("PEDIDO_CREADO | PED-1 | PENDIENTE | 9990", "eventoId=evt-1",
                "origen=pedidos360.pedidos.eventos-0@5");
    }

    @Test
    @DisplayName("Un evento incompleto es un error no recuperable")
    void eventoIncompletoNoRecuperable() {
        AuditoriaService service = new AuditoriaService(tmp.toString(), 100);
        assertThatThrownBy(() -> service.registrar(evento(null, "PED-1"), "x"))
                .isInstanceOf(ErrorNoRecuperableException.class);
        assertThatThrownBy(() -> service.registrar(null, "x"))
                .isInstanceOf(ErrorNoRecuperableException.class);
    }

    @Test
    @DisplayName("Si no se puede escribir el archivo es un error recuperable (se reintenta)")
    void falloDeEscrituraRecuperable() throws Exception {
        Path archivoEnLugarDeCarpeta = Files.writeString(tmp.resolve("no-es-carpeta"), "x");
        AuditoriaService service = new AuditoriaService(archivoEnLugarDeCarpeta.toString(), 100);
        assertThatThrownBy(() -> service.registrar(evento("evt-1", "PED-1"), "x"))
                .isInstanceOf(ErrorRecuperableException.class);
    }

    private static PedidoEvento evento(String eventoId, String numero) {
        return new PedidoEvento("PEDIDO_CREADO", 1L, numero, "Ana", "ana@duocuc.cl", "PENDIENTE",
                9990.0, FECHA, FECHA, List.of(), eventoId);
    }
}
