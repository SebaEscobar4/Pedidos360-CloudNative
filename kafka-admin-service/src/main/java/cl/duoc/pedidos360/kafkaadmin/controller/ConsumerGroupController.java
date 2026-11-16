package cl.duoc.pedidos360.kafkaadmin.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.kafkaadmin.dto.GrupoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.GrupoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsResponse;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;

/**
 * =========================================================================
 * CONTROLADOR: ConsumerGroupController
 * =========================================================================
 *   GET    /api/consumer-groups                            Lista los grupos y su estado           200
 *   GET    /api/consumer-groups/{groupId}                  Miembros, offsets y lag                 200 | 404
 *   POST   /api/consumer-groups/{groupId}/reset-offsets    Reinicia offsets (grupo detenido)       200 | 400 | 404 | 409
 *   DELETE /api/consumer-groups/{groupId}                  Elimina un grupo sin consumidores       204 | 404 | 409
 * CONECTA CON: KafkaAdminService.
 */
@RestController
@RequestMapping("/api/consumer-groups")
public class ConsumerGroupController {

    private final KafkaAdminService kafkaAdminService;

    public ConsumerGroupController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping
    public List<GrupoResumen> listar() {
        return kafkaAdminService.listarGrupos();
    }

    @GetMapping("/{groupId}")
    public GrupoDetalle obtener(@PathVariable String groupId) {
        return kafkaAdminService.describirGrupo(groupId);
    }

    @PostMapping("/{groupId}/reset-offsets")
    public ResetOffsetsResponse reiniciarOffsets(@PathVariable String groupId, @Valid @RequestBody ResetOffsetsRequest request) {
        return kafkaAdminService.reiniciarOffsets(groupId, request);
    }

    @DeleteMapping("/{groupId}")
    public ResponseEntity<Void> eliminar(@PathVariable String groupId) {
        kafkaAdminService.eliminarGrupo(groupId);
        return ResponseEntity.noContent().build();
    }
}
