package cl.duoc.pedidos360.kafkaadmin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminConfig.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.ClusterResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.CrearTopicoRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.GrupoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.GrupoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoResumen;
import cl.duoc.pedidos360.kafkaadmin.exception.KafkaNoDisponibleException;
import cl.duoc.pedidos360.kafkaadmin.exception.OperacionNoPermitidaException;
import cl.duoc.pedidos360.kafkaadmin.exception.RecursoNoEncontradoException;
import cl.duoc.pedidos360.kafkaadmin.exception.RecursoYaExisteException;
import cl.duoc.pedidos360.kafkaadmin.exception.SolicitudInvalidaException;

/**
 * =========================================================================
 * SERVICIO: KafkaAdminService
 * =========================================================================
 * FUNCION: Encapsula TODA la interaccion con el AdminClient de Kafka. Los controladores no
 *          usan clases de Kafka: reciben y devuelven DTO.
 *
 *   Cluster          describir brokers y controlador.
 *   Topicos          crear (particiones, replicas, configs), listar, describir, modificar y
 *                    restablecer configuraciones, aumentar particiones, eliminar.
 *   Consumer groups  listar, describir (miembros, offsets, lag), reiniciar offsets, eliminar.
 *
 *   Cada error del cluster se traduce a una excepcion de negocio (404, 409, 400, 503) y cada
 *   cambio queda registrado en el log con el prefijo [KAFKA-ADMIN].
 * CONECTA CON: TopicController, ConsumerGroupController y ClusterController.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    /** Parametros de administracion que se informan siempre en el detalle de un topico. */
    private static final List<String> CONFIGS_PRINCIPALES = List.of(
            "cleanup.policy", "retention.ms", "retention.bytes", "min.insync.replicas", "segment.ms",
            "compression.type", "max.message.bytes", "delete.retention.ms", "min.compaction.lag.ms",
            "min.cleanable.dirty.ratio");

    private final AdminClient admin;
    private final ValidadorConfiguraciones validador;
    private final KafkaAdminProperties props;

    public KafkaAdminService(AdminClient admin, ValidadorConfiguraciones validador, KafkaAdminProperties props) {
        this.admin = admin;
        this.validador = validador;
        this.props = props;
    }

    // =================================================================== Cluster

    public ClusterResponse describirCluster() {
        var cluster = admin.describeCluster();
        String clusterId = esperar(cluster.clusterId(), "cluster");
        Node controlador = esperar(cluster.controller(), "cluster");
        List<ClusterResponse.Broker> brokers = esperar(cluster.nodes(), "cluster").stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> new ClusterResponse.Broker(n.id(), n.host(), n.port(), n.rack()))
                .toList();
        return new ClusterResponse(clusterId, controlador == null ? null : controlador.id(), brokers);
    }

    // =================================================================== Topicos

    public List<TopicoResumen> listarTopicos(boolean incluirInternos) {
        Set<String> nombres = esperar(admin.listTopics(new ListTopicsOptions().listInternal(incluirInternos)).names(), "topicos");
        if (nombres.isEmpty()) {
            return List.of();
        }
        return esperar(admin.describeTopics(nombres).allTopicNames(), "topicos").values().stream()
                .map(d -> new TopicoResumen(d.name(), d.partitions().size(), factorReplicacion(d), d.isInternal()))
                .sorted(Comparator.comparing(TopicoResumen::name))
                .toList();
    }

    public TopicoDetalle describirTopico(String nombre) {
        TopicDescription descripcion = describir(nombre);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = esperar(admin.describeConfigs(List.of(recurso)).all(), "topico '" + nombre + "'").get(recurso);

        Map<String, String> personalizada = new TreeMap<>();
        Map<String, String> efectiva = new LinkedHashMap<>();
        for (ConfigEntry entrada : config.entries()) {
            if (entrada.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                personalizada.put(entrada.name(), entrada.value());
            }
        }
        CONFIGS_PRINCIPALES.forEach(clave -> {
            ConfigEntry entrada = config.get(clave);
            if (entrada != null) {
                efectiva.put(clave, entrada.value());
            }
        });

        List<TopicoDetalle.Particion> particiones = descripcion.partitions().stream()
                .map(p -> new TopicoDetalle.Particion(
                        p.partition(),
                        p.leader() == null ? null : p.leader().id(),
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()))
                .toList();

        return new TopicoDetalle(nombre, descripcion.isInternal(), particiones.size(), factorReplicacion(descripcion),
                particiones, personalizada, efectiva);
    }

    public TopicoDetalle crearTopico(CrearTopicoRequest request) {
        if (request.partitions() > props.maxParticiones()) {
            throw new SolicitudInvalidaException("partitions no puede superar " + props.maxParticiones());
        }
        int brokers = esperar(admin.describeCluster().nodes(), "cluster").size();
        if (request.replicationFactor() > brokers) {
            throw new SolicitudInvalidaException("replicationFactor (" + request.replicationFactor()
                    + ") no puede superar la cantidad de brokers disponibles (" + brokers + ")");
        }
        validador.validar(request.configs(), (int) request.replicationFactor());

        NewTopic topico = new NewTopic(request.name(), request.partitions(), request.replicationFactor())
                .configs(request.configs());
        esperar(admin.createTopics(List.of(topico)).all(), "topico '" + request.name() + "'");
        log.info("[KAFKA-ADMIN] Topico creado: {} particiones={} replicas={} configs={}",
                request.name(), request.partitions(), request.replicationFactor(), request.configs());
        return describirTopico(request.name());
    }

    /** Establece las configuraciones indicadas (SET incremental): las demas no cambian. */
    public TopicoDetalle modificarConfiguraciones(String nombre, Map<String, String> configs) {
        TopicDescription descripcion = describir(nombre);
        validador.validar(configs, factorReplicacion(descripcion));

        List<AlterConfigOp> operaciones = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue().trim()), AlterConfigOp.OpType.SET))
                .toList();
        alterar(nombre, operaciones);
        log.info("[KAFKA-ADMIN] Configuraciones de {} modificadas: {}", nombre, configs);
        return describirTopico(nombre);
    }

    /** Elimina la configuracion personalizada: el topico vuelve al valor por defecto del broker. */
    public void restablecerConfiguracion(String nombre, String clave) {
        describir(nombre);
        if (!validador.permitidas().contains(clave)) {
            throw new SolicitudInvalidaException("Configuracion no permitida: '" + clave
                    + "'. Se pueden administrar: " + validador.permitidas());
        }
        alterar(nombre, List.of(new AlterConfigOp(new ConfigEntry(clave, null), AlterConfigOp.OpType.DELETE)));
        log.info("[KAFKA-ADMIN] Configuracion {} de {} restablecida al valor por defecto", clave, nombre);
    }

    public TopicoDetalle aumentarParticiones(String nombre, int nuevasParticiones) {
        int actuales = describir(nombre).partitions().size();
        if (nuevasParticiones <= actuales) {
            throw new SolicitudInvalidaException("Kafka solo permite aumentar particiones: el topico tiene "
                    + actuales + " y se pidieron " + nuevasParticiones);
        }
        if (nuevasParticiones > props.maxParticiones()) {
            throw new SolicitudInvalidaException("partitions no puede superar " + props.maxParticiones());
        }
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(nuevasParticiones))).all(),
                "topico '" + nombre + "'");
        log.info("[KAFKA-ADMIN] Particiones de {} aumentadas de {} a {}", nombre, actuales, nuevasParticiones);
        return describirTopico(nombre);
    }

    public void eliminarTopico(String nombre) {
        if (props.topicosProtegidos().contains(nombre)) {
            throw new OperacionNoPermitidaException("El topico '" + nombre
                    + "' esta protegido: lo usan los microservicios de Pedidos360");
        }
        if (nombre.startsWith("__")) {
            throw new OperacionNoPermitidaException("Los topicos internos de Kafka no se pueden eliminar");
        }
        describir(nombre);
        esperar(admin.deleteTopics(List.of(nombre)).all(), "topico '" + nombre + "'");
        log.info("[KAFKA-ADMIN] Topico eliminado: {}", nombre);
    }

    // =================================================================== Consumer groups

    public List<GrupoResumen> listarGrupos() {
        return esperar(admin.listConsumerGroups().all(), "consumer groups").stream()
                .map(g -> new GrupoResumen(g.groupId(), g.state().map(Enum::name).orElse("UNKNOWN")))
                .sorted(Comparator.comparing(GrupoResumen::groupId))
                .toList();
    }

    public GrupoDetalle describirGrupo(String groupId) {
        ConsumerGroupDescription descripcion = describirGrupoKafka(groupId);

        List<GrupoDetalle.Miembro> miembros = descripcion.members().stream()
                .map(m -> new GrupoDetalle.Miembro(m.consumerId(), m.clientId(), m.host(),
                        m.assignment().topicPartitions().stream()
                                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                                .map(TopicPartition::toString)
                                .toList()))
                .sorted(Comparator.comparing(GrupoDetalle.Miembro::clientId))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> confirmados = esperar(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(), "consumer group '" + groupId + "'");

        // Particiones a informar: las que tienen offset confirmado y las asignadas a miembros
        Set<TopicPartition> particiones = new java.util.HashSet<>(confirmados.keySet());
        descripcion.members().forEach(m -> particiones.addAll(m.assignment().topicPartitions()));
        Map<TopicPartition, Long> finales = offsetsFinales(particiones);

        List<GrupoDetalle.OffsetParticion> offsets = new ArrayList<>();
        long lagTotal = 0;
        for (TopicPartition tp : particiones) {
            OffsetAndMetadata confirmado = confirmados.get(tp);
            long fin = finales.getOrDefault(tp, 0L);
            Long lag = confirmado == null ? null : Math.max(0, fin - confirmado.offset());
            lagTotal += lag == null ? 0 : lag;
            offsets.add(new GrupoDetalle.OffsetParticion(tp.topic(), tp.partition(),
                    confirmado == null ? null : confirmado.offset(), fin, lag));
        }
        offsets.sort(Comparator.comparing(GrupoDetalle.OffsetParticion::topic)
                .thenComparingInt(GrupoDetalle.OffsetParticion::partition));

        return new GrupoDetalle(groupId, descripcion.state().name(),
                descripcion.coordinator() == null ? null : descripcion.coordinator().id(),
                descripcion.partitionAssignor(), miembros, offsets, lagTotal);
    }

    /**
     * Mueve los offsets del grupo en todas las particiones del topico. Solo se permite con el grupo
     * sin consumidores activos (estado EMPTY o inexistente), igual que kafka-consumer-groups --reset-offsets.
     */
    public ResetOffsetsResponse reiniciarOffsets(String groupId, ResetOffsetsRequest request) {
        if (request.destino() == ResetOffsetsRequest.Destino.OFFSET && request.offset() == null) {
            throw new SolicitudInvalidaException("Con destino OFFSET se debe indicar 'offset'");
        }
        ConsumerGroupState estado = estadoGrupo(groupId);
        if (estado != ConsumerGroupState.EMPTY && estado != ConsumerGroupState.DEAD) {
            throw new OperacionNoPermitidaException("El consumer group '" + groupId + "' tiene consumidores activos (estado "
                    + estado + "): deten sus microservicios antes de reiniciar los offsets");
        }

        List<TopicPartition> particiones = describir(request.topic()).partitions().stream()
                .map(p -> new TopicPartition(request.topic(), p.partition()))
                .toList();
        Map<TopicPartition, Long> inicio = listarOffsets(particiones, OffsetSpec.earliest());
        Map<TopicPartition, Long> fin = listarOffsets(particiones, OffsetSpec.latest());

        Map<TopicPartition, OffsetAndMetadata> nuevos = new HashMap<>();
        for (TopicPartition tp : particiones) {
            long destino = switch (request.destino()) {
                case EARLIEST -> inicio.get(tp);
                case LATEST -> fin.get(tp);
                // Se acota al rango que existe en la particion
                case OFFSET -> Math.max(inicio.get(tp), Math.min(fin.get(tp), request.offset()));
            };
            nuevos.put(tp, new OffsetAndMetadata(destino));
        }
        esperar(admin.alterConsumerGroupOffsets(groupId, nuevos).all(), "consumer group '" + groupId + "'");

        Map<Integer, Long> resumen = new TreeMap<>();
        nuevos.forEach((tp, o) -> resumen.put(tp.partition(), o.offset()));
        log.info("[KAFKA-ADMIN] Offsets del grupo {} en {} reiniciados a {}: {}", groupId, request.topic(), request.destino(), resumen);
        return new ResetOffsetsResponse(groupId, request.topic(), request.destino().name(), resumen);
    }

    public void eliminarGrupo(String groupId) {
        esperar(admin.deleteConsumerGroups(List.of(groupId)).all(), "consumer group '" + groupId + "'");
        log.info("[KAFKA-ADMIN] Consumer group eliminado: {}", groupId);
    }

    // =================================================================== Apoyo

    private TopicDescription describir(String nombre) {
        return esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), "topico '" + nombre + "'").get(nombre);
    }

    private ConsumerGroupDescription describirGrupoKafka(String groupId) {
        ConsumerGroupDescription descripcion = esperar(admin.describeConsumerGroups(List.of(groupId)).all(),
                "consumer group '" + groupId + "'").get(groupId);
        // Kafka responde DEAD para un grupo que no existe
        if (descripcion.state() == ConsumerGroupState.DEAD) {
            throw new RecursoNoEncontradoException("No existe el consumer group '" + groupId + "'");
        }
        return descripcion;
    }

    private ConsumerGroupState estadoGrupo(String groupId) {
        return esperar(admin.describeConsumerGroups(List.of(groupId)).all(), "consumer group '" + groupId + "'")
                .get(groupId).state();
    }

    private void alterar(String nombre, Collection<AlterConfigOp> operaciones) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, operaciones)).all(), "topico '" + nombre + "'");
    }

    private Map<TopicPartition, Long> offsetsFinales(Collection<TopicPartition> particiones) {
        return particiones.isEmpty() ? Map.of() : listarOffsets(particiones, OffsetSpec.latest());
    }

    private Map<TopicPartition, Long> listarOffsets(Collection<TopicPartition> particiones, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> consulta = particiones.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> spec));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resultado =
                esperar(admin.listOffsets(consulta).all(), "offsets");
        Map<TopicPartition, Long> offsets = new HashMap<>();
        resultado.forEach((tp, info) -> offsets.put(tp, info.offset()));
        return offsets;
    }

    private static int factorReplicacion(TopicDescription descripcion) {
        return descripcion.partitions().stream()
                .map(TopicPartitionInfo::replicas)
                .mapToInt(List::size)
                .max()
                .orElse(0);
    }

    /**
     * Espera el resultado de una operacion del AdminClient y traduce los errores de Kafka a
     * excepciones de negocio con codigo HTTP claro.
     */
    private <T> T esperar(KafkaFuture<T> futuro, String recurso) {
        try {
            return futuro.get(props.timeoutSegundos(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaNoDisponibleException("Operacion interrumpida", e);
        } catch (TimeoutException e) {
            throw new KafkaNoDisponibleException("Kafka no respondio en " + props.timeoutSegundos() + " segundos", e);
        } catch (ExecutionException e) {
            Throwable causa = e.getCause();
            if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
                throw new RecursoNoEncontradoException("No existe el " + recurso, causa);
            }
            if (causa instanceof TopicExistsException) {
                throw new RecursoYaExisteException("Ya existe el " + recurso, causa);
            }
            if (causa instanceof GroupNotEmptyException) {
                throw new OperacionNoPermitidaException("El " + recurso + " tiene consumidores activos", causa);
            }
            if (causa instanceof InvalidReplicationFactorException || causa instanceof InvalidPartitionsException
                    || causa instanceof InvalidConfigurationException || causa instanceof InvalidRequestException
                    || causa instanceof PolicyViolationException || causa instanceof InvalidTopicException) {
                throw new SolicitudInvalidaException("Kafka rechazo la operacion sobre el " + recurso + ": " + causa.getMessage(), causa);
            }
            if (causa instanceof org.apache.kafka.common.errors.TimeoutException) {
                throw new KafkaNoDisponibleException("Kafka no respondio a tiempo", causa);
            }
            throw new KafkaNoDisponibleException("Error de Kafka sobre el " + recurso + ": " + causa.getMessage(), causa);
        }
    }
}
