package cl.duoc.pedidos360.kafkaadmin.controller;

import java.net.URI;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.kafkaadmin.dto.ConfiguracionesRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.CrearTopicoRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ParticionesRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoResumen;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;

/**
 * =========================================================================
 * CONTROLADOR: TopicController
 * =========================================================================
 *   GET    /api/topics?internos=false          Lista los topicos                          200
 *   GET    /api/topics/{name}                  Particiones, ISR y configuraciones          200 | 404
 *   POST   /api/topics                         Crea un topico con sus configuraciones      201 | 400 | 409
 *   PUT    /api/topics/{name}/configs          Modifica configuraciones (retencion, etc.)  200 | 400 | 404
 *   DELETE /api/topics/{name}/configs/{key}    Restablece una configuracion por defecto    204 | 400 | 404
 *   PATCH  /api/topics/{name}/partitions       Aumenta las particiones                     200 | 400 | 404
 *   DELETE /api/topics/{name}                  Elimina un topico (salvo los protegidos)    204 | 404 | 409
 * CONECTA CON: KafkaAdminService.
 */
@RestController
@RequestMapping("/api/topics")
public class TopicController {

    private final KafkaAdminService kafkaAdminService;

    public TopicController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping
    public List<TopicoResumen> listar(@RequestParam(defaultValue = "false") boolean internos) {
        return kafkaAdminService.listarTopicos(internos);
    }

    @GetMapping("/{name}")
    public TopicoDetalle obtener(@PathVariable String name) {
        return kafkaAdminService.describirTopico(name);
    }

    @PostMapping
    public ResponseEntity<TopicoDetalle> crear(@Valid @RequestBody CrearTopicoRequest request) {
        TopicoDetalle creado = kafkaAdminService.crearTopico(request);
        return ResponseEntity.created(URI.create("/api/topics/" + creado.name())).body(creado);
    }

    @PutMapping("/{name}/configs")
    public TopicoDetalle modificarConfiguraciones(@PathVariable String name, @Valid @RequestBody ConfiguracionesRequest request) {
        return kafkaAdminService.modificarConfiguraciones(name, request.configs());
    }

    @DeleteMapping("/{name}/configs/{key}")
    public ResponseEntity<Void> restablecerConfiguracion(@PathVariable String name, @PathVariable String key) {
        kafkaAdminService.restablecerConfiguracion(name, key);
        return ResponseEntity.noContent().build();
    }

    @PatchMapping("/{name}/partitions")
    public TopicoDetalle aumentarParticiones(@PathVariable String name, @Valid @RequestBody ParticionesRequest request) {
        return kafkaAdminService.aumentarParticiones(name, request.partitions());
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> eliminar(@PathVariable String name) {
        kafkaAdminService.eliminarTopico(name);
        return ResponseEntity.noContent().build();
    }
}
