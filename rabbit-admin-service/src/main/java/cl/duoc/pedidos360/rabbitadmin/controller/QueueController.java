package cl.duoc.pedidos360.rabbitadmin.controller;

import java.net.URI;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.rabbitadmin.dto.QueueRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueResponse;
import cl.duoc.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;

/**
 * =========================================================================
 * CONTROLADOR: QueueController
 * =========================================================================
 *   POST   /api/queues          Crea una cola                  201 | 400 | 409
 *   GET    /api/queues/{name}   Consulta el estado de una cola 200 | 404
 *   DELETE /api/queues/{name}   Elimina una cola               204 | 404
 * CONECTA CON: RabbitAdminService.
 */
@RestController
@RequestMapping("/api/queues")
public class QueueController {

    private final RabbitAdminService rabbitAdminService;

    public QueueController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping
    public ResponseEntity<QueueResponse> crear(@Valid @RequestBody QueueRequest request) {
        QueueResponse creada = rabbitAdminService.crearCola(request);
        return ResponseEntity.created(URI.create("/api/queues/" + creada.name())).body(creada);
    }

    @GetMapping("/{name}")
    public QueueResponse obtener(@PathVariable String name) {
        return rabbitAdminService.obtenerCola(name);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> eliminar(@PathVariable String name) {
        rabbitAdminService.eliminarCola(name);
        return ResponseEntity.noContent().build();
    }
}
