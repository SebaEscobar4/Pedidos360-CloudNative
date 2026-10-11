package cl.duoc.pedidos360.rabbitadmin.exception;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import cl.duoc.pedidos360.rabbitadmin.dto.ApiError;
import jakarta.servlet.http.HttpServletRequest;

/**
 * =========================================================================
 * MANEJO DE ERRORES: GlobalExceptionHandler
 * =========================================================================
 * FUNCION: Traduce cada tipo de error a una respuesta JSON clara y al codigo HTTP correcto:
 *          400 validacion | 404 no existe | 409 conflicto | 503 broker caido.
 * CONECTA CON: Todos los controladores del microservicio.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    // @Valid fallo en un body JSON o en parametros de consulta
    @ExceptionHandler(BindException.class)
    public ResponseEntity<ApiError> validacion(BindException ex, HttpServletRequest request) {
        List<String> detalles = ex.getBindingResult().getAllErrors().stream()
                .map(error -> error.getDefaultMessage())
                .sorted()
                .toList();
        return responder(HttpStatus.BAD_REQUEST, "Los datos enviados no son validos", request, detalles);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> jsonIlegible(HttpMessageNotReadableException ex, HttpServletRequest request) {
        return responder(HttpStatus.BAD_REQUEST, "El cuerpo de la solicitud no es un JSON valido", request, List.of());
    }

    @ExceptionHandler(NombreInvalidoException.class)
    public ResponseEntity<ApiError> nombreInvalido(NombreInvalidoException ex, HttpServletRequest request) {
        return responder(HttpStatus.BAD_REQUEST, ex.getMessage(), request, List.of());
    }

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<ApiError> noEncontrado(RecursoNoEncontradoException ex, HttpServletRequest request) {
        return responder(HttpStatus.NOT_FOUND, ex.getMessage(), request, List.of());
    }

    @ExceptionHandler(RecursoYaExisteException.class)
    public ResponseEntity<ApiError> yaExiste(RecursoYaExisteException ex, HttpServletRequest request) {
        return responder(HttpStatus.CONFLICT, ex.getMessage(), request, List.of());
    }

    @ExceptionHandler(AmqpConnectException.class)
    public ResponseEntity<ApiError> brokerCaido(AmqpConnectException ex, HttpServletRequest request) {
        log.error("[RABBIT-ADMIN] Sin conexion con RabbitMQ: {}", ex.getMessage());
        return responder(HttpStatus.SERVICE_UNAVAILABLE, "No hay conexion con RabbitMQ", request, List.of());
    }

    // RabbitMQ rechazo la operacion (por ejemplo, argumentos incompatibles)
    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<ApiError> rechazadoPorBroker(AmqpException ex, HttpServletRequest request) {
        log.error("[RABBIT-ADMIN] RabbitMQ rechazo la operacion", ex);
        return responder(HttpStatus.CONFLICT, "RabbitMQ rechazo la operacion solicitada", request, List.of());
    }

    private ResponseEntity<ApiError> responder(
            HttpStatus status, String mensaje, HttpServletRequest request, List<String> detalles) {
        ApiError body = ApiError.of(status.value(), status.getReasonPhrase(), mensaje, request.getRequestURI(), detalles);
        return ResponseEntity.status(status).body(body);
    }
}
