package cl.duoc.pedidos360.notificaciones.ticket;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notificaciones.messaging.ErrorNoRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.ErrorRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * =========================================================================
 * SERVICIO: TicketService (dominio 'ticket')
 * =========================================================================
 * FUNCION: Genera el ticket de compra de un pedido como archivo de texto
 *          (TICKET-PED-XXXXXXXX.txt) dentro del directorio configurado.
 *          No conoce RabbitMQ: recibe un PedidoEvento ya convertido.
 * CONECTA CON: TicketPedidoConsumer.
 */
@Service
public class TicketService {

    private static final Logger log = LoggerFactory.getLogger(TicketService.class);

    // El numero de pedido forma parte del nombre del archivo: solo se aceptan caracteres seguros
    private static final Pattern NUMERO_PEDIDO = Pattern.compile("^[A-Za-z0-9-]{1,40}$");
    private static final DateTimeFormatter FECHA = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm");

    private final Path directorio;

    public TicketService(@Value("${app.tickets.directorio}") String directorio) {
        this.directorio = Path.of(directorio);
    }

    public Path generar(PedidoEvento evento) {
        validar(evento);
        Path archivo = directorio.resolve("TICKET-" + evento.numeroPedido() + ".txt");
        try {
            Files.createDirectories(directorio);
            Files.writeString(archivo, armarContenido(evento), StandardCharsets.UTF_8);
        } catch (IOException e) {
            // Disco lleno, carpeta bloqueada, etc.: puede resolverse en un reintento
            throw new ErrorRecuperableException("No se pudo escribir el ticket " + archivo, e);
        }
        log.info("[TICKET] Ticket generado: {}", archivo.toAbsolutePath());
        return archivo;
    }

    private void validar(PedidoEvento evento) {
        if (!NUMERO_PEDIDO.matcher(evento.numeroPedido()).matches()) {
            throw new ErrorNoRecuperableException("Numero de pedido invalido para generar ticket");
        }
        if (evento.items() == null || evento.items().isEmpty() || evento.total() == null) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.numeroPedido() + " no trae items o total para generar el ticket");
        }
    }

    private String armarContenido(PedidoEvento evento) {
        StringBuilder sb = new StringBuilder();
        sb.append("========================================\n");
        sb.append("        PEDIDOS360 - TICKET DE COMPRA\n");
        sb.append("========================================\n");
        sb.append("Pedido : ").append(evento.numeroPedido()).append('\n');
        sb.append("Cliente: ").append(evento.clienteNombre()).append('\n');
        sb.append("Email  : ").append(evento.clienteEmail()).append('\n');
        if (evento.fechaCreacion() != null) {
            sb.append("Fecha  : ").append(evento.fechaCreacion().format(FECHA)).append('\n');
        }
        sb.append("----------------------------------------\n");
        for (PedidoEvento.Item item : evento.items()) {
            sb.append(String.format("%2d x %-22s $%,10.0f%n",
                    item.cantidad(), recortar(item.productoNombre()), item.subtotal()));
        }
        sb.append("----------------------------------------\n");
        sb.append(String.format("TOTAL %27s $%,10.0f%n", "", evento.total()));
        sb.append("========================================\n");
        return sb.toString();
    }

    private String recortar(String texto) {
        if (texto == null) {
            return "";
        }
        return texto.length() <= 22 ? texto : texto.substring(0, 22);
    }
}
