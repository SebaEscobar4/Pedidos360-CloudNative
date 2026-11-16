package cl.duoc.pedidos360.notificaciones.auditoria;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notificaciones.messaging.ErrorNoRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.ErrorRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * =========================================================================
 * SERVICIO: AuditoriaService (dominio 'auditoria')
 * =========================================================================
 * FUNCION: Deja un registro de auditoria por cada evento de pedido: un archivo de texto por dia
 *          (auditoria/pedidos-AAAA-MM-DD.txt) con una linea por evento.
 *            - Payload incompleto            -> ErrorNoRecuperableException (va al DLT sin reintentos).
 *            - No se pudo escribir el archivo -> ErrorRecuperableException (se reintenta).
 *            - Evento repetido (eventoId)     -> se ignora: la auditoria no queda con lineas duplicadas.
 *          No conoce Kafka: recibe el evento y el origen (topico-particion@offset) como texto.
 * CONECTA CON: AuditoriaPedidoListener.
 */
@Service
public class AuditoriaService {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);

    private final Path directorio;
    private final Set<String> registrados;

    public AuditoriaService(
            @Value("${app.auditoria.directorio}") String directorio,
            @Value("${app.auditoria.capacidad-idempotencia}") int capacidad) {
        this.directorio = Path.of(directorio);
        this.registrados = Collections.synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > capacidad;
            }
        }));
    }

    /** @return true si se escribio una linea nueva; false si el evento ya estaba auditado. */
    public boolean registrar(PedidoEvento evento, String origen) {
        validar(evento);
        if (registrados.contains(evento.eventoId())) {
            log.info("[AUDITORIA] Evento {} ya auditado, se ignora", evento.eventoId());
            return false;
        }

        LocalDate dia = evento.fechaEvento().toLocalDate();
        String linea = String.join(" | ",
                LocalDateTime.now().toString(),
                evento.fechaEvento().toString(),
                evento.tipo(),
                evento.numeroPedido(),
                evento.estado(),
                evento.total() == null ? "-" : String.valueOf(Math.round(evento.total())),
                evento.clienteEmail() == null ? "-" : evento.clienteEmail(),
                "eventoId=" + evento.eventoId(),
                "origen=" + origen) + System.lineSeparator();

        Path archivo = directorio.resolve("pedidos-" + dia + ".txt");
        try {
            Files.createDirectories(directorio);
            Files.writeString(archivo, linea, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new ErrorRecuperableException("No se pudo escribir la auditoria en " + archivo, e);
        }
        registrados.add(evento.eventoId());
        log.info("[AUDITORIA] {} {} estado={} registrado en {} (origen {})",
                evento.tipo(), evento.numeroPedido(), evento.estado(), archivo.getFileName(), origen);
        return true;
    }

    private static void validar(PedidoEvento evento) {
        if (evento == null) {
            throw new ErrorNoRecuperableException("El mensaje no trae cuerpo");
        }
        if (vacio(evento.eventoId()) || vacio(evento.tipo()) || vacio(evento.numeroPedido())
                || vacio(evento.estado()) || evento.fechaEvento() == null) {
            throw new ErrorNoRecuperableException("Evento de auditoria incompleto: se requieren eventoId, tipo, "
                    + "numeroPedido, estado y fechaEvento (pedido=" + evento.numeroPedido() + ")");
        }
    }

    private static boolean vacio(String valor) {
        return valor == null || valor.isBlank();
    }
}
