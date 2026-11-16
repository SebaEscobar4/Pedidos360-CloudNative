#!/usr/bin/env bash
# =========================================================================
# Script de infraestructura: topicos Kafka de Pedidos360
# =========================================================================
# Crea los topicos si no existen y (re)aplica sus configuraciones, de modo que el
# cluster quede siempre igual sin importar cuantas veces se ejecute.
#
# Lo ejecuta el contenedor 'kafka-init' de docker-compose.kafka.yml. Tambien puede
# correrse a mano:
#   docker compose -f docker-compose.kafka.yml run --rm kafka-init
#
# Diseno de cada flujo de datos:
#
#  pedidos360.pedidos.eventos      Flujo de eventos de pedidos (pedido creado / cambio de estado).
#                                  3 particiones (clave = numero de pedido: orden garantizado por pedido
#                                  y hasta 3 consumidores en paralelo por grupo), 3 replicas,
#                                  min.insync.replicas=2, retencion 7 dias, cleanup=delete (es un log de hechos).
#
#  pedidos360.metricas.ventas      Ultima foto de las metricas de ventas por dia y por particion de origen
#                                  (clave = fecha#pN). cleanup=compact: Kafka conserva solo el ultimo valor
#                                  de cada clave, asi un consumidor nuevo reconstruye el estado leyendo desde el inicio.
#
#  *.DLT                           Dead Letter Topics: mensajes que no se pudieron procesar.
#                                  1 particion (bajo volumen), retencion 14 dias para revisarlos con calma.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"

DIA_MS=86400000

# crear_topico <nombre> <particiones> <replicas> <configs separadas por coma>
crear_topico() {
  local nombre="$1" particiones="$2" replicas="$3" configs="$4"

  echo ">> Topico ${nombre} (particiones=${particiones}, replicas=${replicas})"
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$nombre" --partitions "$particiones" --replication-factor "$replicas"

  # Se reaplican siempre para que un cambio en este script llegue a un topico ya existente
  kafka-configs --bootstrap-server "$BOOTSTRAP" --alter \
    --entity-type topics --entity-name "$nombre" --add-config "$configs"
}

crear_topico pedidos360.pedidos.eventos 3 3 \
  "cleanup.policy=delete,retention.ms=$((7 * DIA_MS)),min.insync.replicas=2,compression.type=producer,max.message.bytes=1048576"

crear_topico pedidos360.pedidos.eventos.DLT 1 3 \
  "cleanup.policy=delete,retention.ms=$((14 * DIA_MS)),min.insync.replicas=2"

crear_topico pedidos360.metricas.ventas 3 3 \
  "cleanup.policy=compact,min.insync.replicas=2,min.compaction.lag.ms=60000,segment.ms=3600000,min.cleanable.dirty.ratio=0.1,delete.retention.ms=${DIA_MS}"

crear_topico pedidos360.metricas.ventas.DLT 1 3 \
  "cleanup.policy=delete,retention.ms=$((14 * DIA_MS)),min.insync.replicas=2"

echo
echo "=== Topicos del cluster ==="
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topics-with-overrides --exclude-internal
