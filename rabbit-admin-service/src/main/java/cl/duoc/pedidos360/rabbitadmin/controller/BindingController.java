package cl.duoc.pedidos360.rabbitadmin.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;

/**
 * =========================================================================
 * CONTROLADOR: BindingController
 * =========================================================================
 *   POST   /api/bindings                                  Crea un binding    201 | 400 | 404
 *   DELETE /api/bindings?queue=&exchange=&routingKey=     Elimina un binding 204 | 400 | 404
 *
 * Un binding no tiene identificador propio: se identifica por cola + exchange + routing key,
 * por eso el DELETE los recibe como parametros de consulta.
 * CONECTA CON: RabbitAdminService.
 */
@RestController
@RequestMapping("/api/bindings")
public class BindingController {

    private final RabbitAdminService rabbitAdminService;

    public BindingController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping
    public ResponseEntity<BindingRequest> crear(@Valid @RequestBody BindingRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(rabbitAdminService.crearBinding(request));
    }

    @DeleteMapping
    public ResponseEntity<Void> eliminar(@Valid @ModelAttribute BindingRequest request) {
        rabbitAdminService.eliminarBinding(request);
        return ResponseEntity.noContent().build();
    }
}
