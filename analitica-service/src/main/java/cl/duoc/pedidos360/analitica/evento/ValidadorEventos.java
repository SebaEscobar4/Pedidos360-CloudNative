package cl.duoc.pedidos360.analitica.evento;

import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.analitica.evento.PedidoEventoKafka.TipoEvento;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

/**
 * =========================================================================
 * VALIDACION: ValidadorEventos
 * =========================================================================
 * FUNCION: Valida el payload de cada evento antes de que llegue a la logica de negocio:
 *            1. Reglas de Bean Validation declaradas en PedidoEventoKafka.
 *            2. Reglas que dependen del tipo: un PEDIDO_CREADO debe traer total e items.
 *          Si algo falla lanza EventoInvalidoException (no recuperable -> DLT).
 * CONECTA CON: PedidoEventosListener.
 */
@Component
public class ValidadorEventos {

    private final Validator validator;

    public ValidadorEventos(Validator validator) {
        this.validator = validator;
    }

    public PedidoEventoKafka validar(PedidoEventoKafka evento) {
        if (evento == null) {
            throw new EventoInvalidoException("El mensaje no trae cuerpo");
        }

        var violaciones = validator.validate(evento);
        if (!violaciones.isEmpty()) {
            String detalle = violaciones.stream()
                    .map(ConstraintViolation::getMessage)
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new EventoInvalidoException("Evento " + evento.eventoId() + " invalido: " + detalle);
        }

        if (evento.tipo() == TipoEvento.PEDIDO_CREADO) {
            if (evento.total() == null) {
                throw new EventoInvalidoException("El PEDIDO_CREADO " + evento.numeroPedido() + " no trae total");
            }
            if (evento.items() == null || evento.items().isEmpty()) {
                throw new EventoInvalidoException("El PEDIDO_CREADO " + evento.numeroPedido() + " no trae items");
            }
        }
        return evento;
    }
}
