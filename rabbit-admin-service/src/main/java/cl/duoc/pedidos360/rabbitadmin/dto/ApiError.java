package cl.duoc.pedidos360.rabbitadmin.dto;

import java.time.Instant;
import java.util.List;

/**
 * Formato unico de error de la API. 'details' lista cada campo invalido cuando falla la validacion.
 */
public record ApiError(String timestamp, int status, String error, String message, String path, List<String> details) {

    public static ApiError of(int status, String error, String message, String path, List<String> details) {
        return new ApiError(Instant.now().toString(), status, error, message, path, details);
    }
}
