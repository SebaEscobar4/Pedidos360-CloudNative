package cl.duoc.pedidos360.rabbitadmin.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeResponse;
import cl.duoc.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;

/**
 * =========================================================================
 * CONTROLADOR: ExchangeController
 * =========================================================================
 *   POST   /api/exchanges          Crea un exchange    201 | 400 | 409
 *   DELETE /api/exchanges/{name}   Elimina un exchange 204 | 404
 * CONECTA CON: RabbitAdminService.
 */
@RestController
@RequestMapping("/api/exchanges")
public class ExchangeController {

    private final RabbitAdminService rabbitAdminService;

    public ExchangeController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping
    public ResponseEntity<ExchangeResponse> crear(@Valid @RequestBody ExchangeRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(rabbitAdminService.crearExchange(request));
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> eliminar(@PathVariable String name) {
        rabbitAdminService.eliminarExchange(name);
        return ResponseEntity.noContent().build();
    }
}
