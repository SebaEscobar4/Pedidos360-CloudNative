package cl.duoc.pedidos360.kafkaadmin.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.pedidos360.kafkaadmin.dto.ClusterResponse;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;

/**
 * =========================================================================
 * CONTROLADOR: ClusterController
 * =========================================================================
 *   GET /api/cluster   Brokers en linea y controlador del cluster   200 | 503
 * CONECTA CON: KafkaAdminService.
 */
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final KafkaAdminService kafkaAdminService;

    public ClusterController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping
    public ClusterResponse describir() {
        return kafkaAdminService.describirCluster();
    }
}
