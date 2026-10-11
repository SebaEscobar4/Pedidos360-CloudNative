package cl.duoc.pedidos360.notificaciones.notificacion;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notificaciones.messaging.ErrorNoRecuperableException;
import cl.duoc.pedidos360.notificaciones.messaging.PedidoEvento;

/**
 * =========================================================================
 * SERVICIO: NotificacionService (dominio 'notificacion')
 * =========================================================================
 * FUNCION: Logica de negocio de las notificaciones al cliente. Arma el mensaje segun el
 *          tipo de evento y simula el envio del correo dejandolo registrado en el log.
 *          No conoce RabbitMQ: recibe un PedidoEvento ya convertido.
 * CONECTA CON: NotificacionPedidoConsumer.
 */
@Service
public class NotificacionService {

    private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    public void notificar(PedidoEvento evento) {
        if (evento.clienteEmail() == null || !EMAIL.matcher(evento.clienteEmail()).matches()) {
            throw new ErrorNoRecuperableException(
                    "El pedido " + evento.numeroPedido() + " no tiene un correo de cliente valido");
        }

        if (PedidoEvento.TIPO_PEDIDO_CREADO.equals(evento.tipo()) && evento.total() == null) {
            throw new ErrorNoRecuperableException("El pedido " + evento.numeroPedido() + " no trae total");
        }

        String asunto;
        String cuerpo;
        if (PedidoEvento.TIPO_PEDIDO_CREADO.equals(evento.tipo())) {
            asunto = "Recibimos tu pedido " + evento.numeroPedido();
            cuerpo = "Hola " + evento.clienteNombre() + ", tu pedido por $" + Math.round(evento.total())
                    + " fue recibido y esta " + evento.estado() + ".";
        } else {
            asunto = "Tu pedido " + evento.numeroPedido() + " cambio de estado";
            cuerpo = "Hola " + evento.clienteNombre() + ", tu pedido ahora esta " + evento.estado() + ".";
        }

        // Simulacion del envio de correo
        log.info("[NOTIFICACION] Para: {} | Asunto: {} | {}", evento.clienteEmail(), asunto, cuerpo);
    }
}
