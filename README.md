# Pedidos360 - Desarrollo Cloud Native I (DSY1107)

Sistema de pedidos de comida.

- **Evaluacion Parcial N° 3:** se incorpora RabbitMQ para procesar de forma asincrona las notificaciones
  al cliente y la generacion de tickets, sin modificar la logica existente del sistema.
- **Evaluacion Parcial N° 5:** se incorpora Apache Kafka para el streaming de eventos de pedidos y la
  analitica de ventas, sin afectar el flujo de RabbitMQ.

## Componentes

| Carpeta | Que es | Puerto | Rol en la mensajeria |
|---|---|---|---|
| `frontend/` | SPA Angular con login en Microsoft Entra ID (IDaaS) | 4200 | - |
| `backend/` | Microservicio de pedidos (Spring Boot + JPA + JWT) | 8080 | **Productor** RabbitMQ y Kafka (eventos de pedidos). **Consumidor** Kafka de metricas |
| `notificaciones-service/` | Microservicio de notificaciones, tickets y auditoria | 8082 | **Consumidor** RabbitMQ (ACK manual, reintentos y DLQ) y **consumidor** Kafka de auditoria |
| `analitica-service/` | Microservicio de analitica de ventas | 8084 | **Consumidor** Kafka de pedidos y **productor** Kafka de metricas |
| `rabbit-admin-service/` | Microservicio administrador de RabbitMQ (protegido con JWT, rol ADMIN) | 8081 | API REST de colas, exchanges y bindings |
| `kafka-admin-service/` | Microservicio administrador de Kafka (protegido con JWT, rol ADMIN) | 8083 | API REST de topicos, configuraciones y consumer groups |
| `docker-compose.yml` | Broker RabbitMQ con consola de administracion | 5672 / 15672 | - |
| `docker-compose.kafka.yml` | Cluster Kafka de 3 brokers + Zookeeper + Kafka-UI | 9092-9094 / 8090 | - |
| `kafka/crear-topicos.sh` | Script de infraestructura que crea los topicos y aplica su configuracion | - | - |

## Flujo de mensajeria (RabbitMQ)

```
                          pedidos360.pedidos.exchange (topic)
 backend  --publica-->   |
 (pedido.creado /        |--(pedido.#)-------> pedidos360.notificaciones.queue --> NotificacionPedidoConsumer
  pedido.estado.         |                              | NACK / reintentos agotados
  actualizado)           |                              v
                         |                     pedidos360.dlx --(notificaciones.dlq)--> pedidos360.notificaciones.dlq
                         |
                         |--(pedido.creado)--> pedidos360.tickets.queue --> TicketPedidoConsumer
                                                        | NACK / reintentos agotados
                                                        v
                                               pedidos360.dlx --(tickets.dlq)--> pedidos360.tickets.dlq
```

- Crear un pedido (`POST /api/pedidos`) publica `pedido.creado`: se notifica al cliente **y** se genera su ticket.
- Cambiar su estado (`PATCH /api/admin/pedidos/{id}/estado`) publica `pedido.estado.actualizado`: solo se notifica.
- El evento se publica **despues del commit** de la transaccion. Si RabbitMQ no esta disponible, el pedido
  se guarda igual y el error queda en el log: la mensajeria no afecta la logica de negocio.

## Streaming y analitica con Kafka (EP5)

### Por que Kafka ademas de RabbitMQ

Los dos brokers resuelven problemas distintos y conviven sobre el mismo evento de Spring que emite `PedidoService`:

| | RabbitMQ | Kafka |
|---|---|---|
| Modelo | Cola de trabajo: el mensaje se borra al confirmarse (ACK) | Registro persistente: el mensaje queda en el topico durante su retencion |
| Uso en Pedidos360 | Tareas puntuales: notificar al cliente y generar el ticket | Historial de eventos que se puede volver a leer: analitica de ventas y auditoria |
| Varios consumidores | Una cola por caso de uso, con bindings | Varios consumer groups leen el mismo topico, cada uno a su ritmo |
| Reprocesar | No (el mensaje ya no existe) | Si: se mueven los offsets o se lee desde el inicio |

Por eso RabbitMQ no se modifico: Kafka se agrego como un segundo publicador (`PedidoKafkaPublisher`) que
escucha el mismo evento despues del commit. Si Kafka no esta disponible, el pedido se guarda igual, RabbitMQ
sigue funcionando y el error queda en el log.

### Flujo

```
                                        pedidos360.pedidos.eventos (3 particiones, clave = numero de pedido)
 backend --PedidoEvento------------->  |
 (PedidoKafkaPublisher)                |--> grupo pedidos360-analitica (3 hilos) --> analitica-service
                                       |        calcula metricas por dia y por particion
                                       |                 |
                                       |                 v
                                       |     pedidos360.metricas.ventas (compact, clave = fecha#pN)
                                       |                 |
                                       |                 v
                                       |     grupo pedidos360-backend-metricas-<host> --> backend
                                       |        GET /api/admin/metricas (rol ADMIN)
                                       |
                                       |--> grupo pedidos360-auditoria (3 hilos) --> notificaciones-service
                                                escribe auditoria/pedidos-AAAA-MM-DD.txt

 Errores (reintentos agotados o mensaje invalido):
   consumidores de pedidos  --> pedidos360.pedidos.eventos.DLT --> grupo pedidos360-analitica-dlt (log)
   consumidor de metricas   --> pedidos360.metricas.ventas.DLT
```

### Topicos y sus parametros

Los crea `kafka/crear-topicos.sh` (contenedor `kafka-init`) y tambien los declaran los servicios con beans
`NewTopic`. Ambos son idempotentes. El cluster tiene `auto.create.topics.enable=false`, asi ningun topico
se crea por error con valores por defecto.

| Topico | Particiones | Replicas | Configuracion | Justificacion |
|---|---|---|---|---|
| `pedidos360.pedidos.eventos` | 3 | 3 | `cleanup.policy=delete`, retencion 7 dias, `min.insync.replicas=2`, `compression.type=producer` | Es un log de hechos. La clave (numero de pedido) garantiza el orden de los eventos de un mismo pedido; 3 particiones permiten hasta 3 consumidores en paralelo por grupo |
| `pedidos360.metricas.ventas` | 3 | 3 | `cleanup.policy=compact`, `min.compaction.lag.ms=60000`, `segment.ms=3600000`, `min.cleanable.dirty.ratio=0.1`, `min.insync.replicas=2` | Solo importa la ultima foto de cada clave: la compactacion la conserva y un consumidor nuevo reconstruye la vista leyendo desde el inicio |
| `pedidos360.pedidos.eventos.DLT` | 1 | 3 | `cleanup.policy=delete`, retencion 14 dias | Bajo volumen; se guardan mas tiempo para revisarlos |
| `pedidos360.metricas.ventas.DLT` | 1 | 3 | `cleanup.policy=delete`, retencion 14 dias | Igual que el anterior, para el consumidor de metricas |

Con 3 replicas y `min.insync.replicas=2`, mas `acks=all` en el productor, un mensaje confirmado esta
escrito en al menos 2 brokers: el cluster tolera la caida de uno sin perder datos ni dejar de aceptar escrituras.

### Consumer groups

| Grupo | Servicio | Estrategia |
|---|---|---|
| `pedidos360-analitica` | analitica-service | Balanceo de carga: los 3 hilos (o varias instancias) se reparten las particiones. Funciona con varias instancias porque el estado se calcula por particion |
| `pedidos360-auditoria` | notificaciones-service | Grupo distinto: recibe **todos** los eventos sin quitarselos a la analitica (publicar/suscribir) |
| `pedidos360-backend-metricas-<host>` | backend | Un grupo por instancia: cada instancia necesita todas las particiones para armar su vista completa |
| `pedidos360-analitica-dlt` | analitica-service | Lee el DLT de pedidos y registra cada mensaje fallido indicando que grupo fallo y por que |

Los consumidores usan asignacion `CooperativeStickyAssignor` (un rebalanceo no detiene a todo el grupo),
commit manual de offsets por el contenedor de Spring y `isolation.level=read_committed`.

### Decision de diseno: estado por particion

analitica-service guarda las metricas en memoria. En una primera version, al reiniciarse empezaba de cero y
publicaba cifras mas bajas que pisaban las correctas. Se resolvio asi:

- Las metricas se calculan **por dia y por particion**, y se publican con la clave `fecha#pN`.
- Al recibir una particion (`ConsumerSeekAware`), el servicio descarta su estado y la relee desde el inicio;
  al perderla en un rebalanceo, la descarta. Asi cada instancia solo es duena de las particiones que tiene.
- `ControlReconstruccion` evita reenviar al DLT los mensajes viejos que se releen al reconstruir.
- El backend suma las fotos parciales del mismo dia al responder `GET /api/admin/metricas`.
- Cada evento lleva un `eventoId` (UUID) y los consumidores descartan duplicados.

## Donde esta cada punto de la pauta

### EP3 - RabbitMQ

| Indicador | Donde verlo |
|---|---|
| Nombres de colas, exchanges y bindings centralizados | Bloque `app.rabbitmq` de `backend/src/main/resources/application.yml` y `notificaciones-service/src/main/resources/application.yml`, leido por `config/RabbitMQProperties.java`. Ninguna otra clase escribe nombres literales. |
| Beans `Queue`, `Exchange` y `Binding` por caso de uso | `config/RabbitMQConfig.java` (en `backend` y en `notificaciones-service`), separado por caso de uso: notificaciones y tickets. |
| Negocio separado de la configuracion de mensajeria | `PedidoService` solo emite un evento de Spring; `messaging/PedidoEventPublisher` es quien habla con RabbitMQ. En el consumidor, `NotificacionService` y `TicketService` no conocen RabbitMQ. |
| Consumidores con `@RabbitListener` agrupados por dominio | `notificaciones-service`: paquetes `notificacion/`, `ticket/` y `dlq/`. |
| ACK y manejo explicito de errores | `acknowledge-mode: manual`. Cada consumidor hace `basicAck` al terminar bien; error no recuperable -> `basicNack` sin requeue -> DLQ; error recuperable -> reintento (hasta 3) y luego DLQ. Ver `messaging/ManejadorErroresMensajeria.java`. |
| Mensajes no entregados a DLQ y registrados en logs | `dlq/DeadLetterConsumer.java` registra cada mensaje muerto con su cola de origen y motivo. El productor registra ademas los mensajes que el broker no pudo enrutar (`ReturnsCallback`). |
| Endpoints REST del administrador | `rabbit-admin-service`: `controller/QueueController`, `ExchangeController`, `BindingController`. |
| Logica encapsulada en `RabbitAdminService` | `rabbit-admin-service/.../service/RabbitAdminService.java`. Los controladores no usan clases de RabbitMQ. |
| Validacion de parametros de entrada | DTO con Bean Validation en `rabbit-admin-service/.../dto/` y respuestas de error claras en `exception/GlobalExceptionHandler.java`. |

### EP5 - Kafka

| Indicador | Donde verlo |
|---|---|
| Configuracion completa del cliente Kafka | Bloque `spring.kafka` de `application.yml` en `backend`, `analitica-service` y `notificaciones-service`: 3 bootstrap servers, productor con `acks=all`, idempotencia, `lz4`, `linger.ms`, `max.block.ms`; consumidor con `earliest`, auto-commit apagado, `read_committed` y `CooperativeStickyAssignor`. Nombres y parametros de topicos en el bloque `app.kafka`, leido por `KafkaTopicosProperties` / `KafkaAnaliticaProperties` / `KafkaAuditoriaProperties`. |
| Productores y consumidores con DTO en los dos topicos | `pedidos.eventos`: produce `backend/.../messaging/PedidoKafkaPublisher.java` (`PedidoEvento`), consumen `analitica-service/.../messaging/PedidoEventosListener.java` (`PedidoEventoKafka`) y `notificaciones-service/.../auditoria/AuditoriaPedidoListener.java`. `metricas.ventas`: produce `analitica-service/.../messaging/MetricasVentasPublisher.java` (`MetricaVentasDia`), consume `backend/.../messaging/MetricasVentasListener.java` (`MetricaVentasDTO`). |
| Administrador de topicos y configuraciones | `kafka/crear-topicos.sh` y beans `NewTopic` en `backend/.../config/KafkaProducerConfig.java` y `analitica-service/.../config/KafkaTopicsConfig.java`. |
| Productores con DTO y serializers | `KafkaTemplate<String, PedidoEvento>` con `StringSerializer` (clave) y `JsonSerializer` (valor, con el `ObjectMapper` de Spring y sin cabeceras de tipo) en `backend/.../config/KafkaProducerConfig.java`. En analitica, `KafkaProducerConfig.java` define el template de metricas y uno para el DLT con `DelegatingByTypeSerializer` (`byte[]` u objeto). |
| Consumidores `@KafkaListener` con dominio separado | La logica no conoce Kafka: `analitica-service/.../metricas/MetricasVentasService.java`, `notificaciones-service/.../auditoria/AuditoriaService.java` y `backend/.../service/MetricasVentasStore.java`. Los listeners estan en `messaging/` o `auditoria/` y solo traducen el mensaje. |
| Manejo de errores: validacion, logs y reintentos | `ErrorHandlingDeserializer` + `JsonDeserializer` (un mensaje que no es JSON no bloquea la particion). Validacion con Bean Validation en `evento/PedidoEventoKafka.java` y `evento/ValidadorEventos.java`. `DefaultErrorHandler` con backoff exponencial (1 s, 2 s, 4 s) y `DeadLetterPublishingRecoverer` hacia el DLT en `analitica-service/.../config/KafkaConsumerConfig.java` y `notificaciones-service/.../config/KafkaAuditoriaConfig.java`; las excepciones no recuperables (`EventoInvalidoException`, `ErrorNoRecuperableException`) van directo al DLT. Logs `[KAFKA-REINTENTO]` y `[KAFKA-DLT]`; `messaging/DeadLetterListener.java` registra cada mensaje del DLT con grupo, topico de origen y motivo. |
| Parametros de topicos: retencion, cleanup, particiones y replicas | Tabla *Topicos y sus parametros* de este README, `kafka/crear-topicos.sh` y bloque `app.kafka.topicos` de cada `application.yml`. |
| Consumer groups | Tabla *Consumer groups* de este README. `group-id` en `spring.kafka.consumer` y `concurrency` del listener en cada servicio; grupo por instancia del backend en `app.kafka.consumo.grupo-metricas`. |
| Administracion de topicos y consumer groups | `kafka-admin-service`: `controller/TopicController`, `ConsumerGroupController`, `ClusterController`; logica con `AdminClient` encapsulada en `service/KafkaAdminService.java`; configuraciones permitidas y sus valores en `service/ValidadorConfiguraciones.java`. |

## Como ejecutar

Requisitos: Java 21, Docker, Node 20+ (para el frontend).

**1. Levantar RabbitMQ**

```bash
docker compose up -d
```

Consola: http://localhost:15672 (usuario `guest`, clave `guest`).

**2. Levantar el cluster Kafka**

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Levanta Zookeeper, los brokers `kafka1`, `kafka2` y `kafka3` (puertos 9092, 9093 y 9094), el contenedor
`kafka-init`, que crea los topicos y termina, y Kafka-UI en http://localhost:8090. Para volver a aplicar
la configuracion de los topicos:

```bash
docker compose -f docker-compose.kafka.yml run --rm kafka-init
```

**3. Levantar los microservicios** (cada uno en su propia terminal)

```bash
cd backend && ./mvnw spring-boot:run
```

```bash
cd notificaciones-service && ./mvnw spring-boot:run
```

```bash
cd analitica-service && ./mvnw spring-boot:run
```

```bash
cd rabbit-admin-service && ./mvnw spring-boot:run
```

```bash
cd kafka-admin-service && ./mvnw spring-boot:run
```

**4. Levantar el frontend**

```bash
cd frontend && npm install && npm start
```

### Variables de entorno de RabbitMQ (backend, notificaciones-service y rabbit-admin-service)

| Variable | Valor por defecto |
|---|---|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USERNAME` | `guest` |
| `RABBITMQ_PASSWORD` | `guest` |
| `RABBITMQ_VHOST` | `/` |

Propias de `notificaciones-service`:

| Variable | Para que sirve | Valor por defecto |
|---|---|---|
| `RABBITMQ_REINTENTOS_MAXIMO` | Reintentos de un error recuperable antes de ir a la DLQ | `3` |
| `DLQ_LISTENER_HABILITADO` | `true`: consume las DLQ y registra los mensajes en el log. `false`: los mensajes quedan en la DLQ para verlos en la consola | `true` |
| `TICKETS_DIR` | Carpeta donde se escriben los tickets | `tickets` |

### Variables de entorno de Kafka

| Variable | Servicios | Para que sirve | Valor por defecto |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | todos los que usan Kafka | Brokers del cluster | `localhost:9092,localhost:9093,localhost:9094` |
| `KAFKA_ENABLED` | backend, notificaciones-service | `false` apaga productores, consumidores y creacion de topicos (por ejemplo, en un servidor sin Kafka) | `true` |
| `KAFKA_GROUP_ID` | analitica-service, notificaciones-service | Consumer group del servicio | `pedidos360-analitica` / `pedidos360-auditoria` |
| `KAFKA_CONCURRENCIA` | analitica-service, notificaciones-service | Hilos consumidores del listener (maximo util = particiones) | `3` |
| `KAFKA_REINTENTOS_MAXIMO` | analitica-service, notificaciones-service | Reintentos de un error recuperable antes del DLT | `3` |
| `AUDITORIA_DIR` | notificaciones-service | Carpeta del archivo de auditoria | `auditoria` |

## Como demostrar que funciona

### RabbitMQ

**Flujo normal.** Con todo arriba, inicia sesion en el frontend y crea un pedido. En la terminal de
`notificaciones-service` aparecen `[NOTIFICACION] Para: ...` y `[TICKET] Ticket generado: ...`, y el
archivo queda en `notificaciones-service/tickets/TICKET-PED-XXXXXXXX.txt`. Si un administrador cambia
el estado del pedido, aparece una nueva `[NOTIFICACION]`.

**DLQ.** En la consola de RabbitMQ: *Exchanges* -> `pedidos360.pedidos.exchange` -> *Publish message*,
routing key `pedido.creado` y como payload un texto que no sea JSON (por ejemplo `hola`). En el log se ve:

```
[DLQ] Mensaje rechazado en cola 'pedidos360.tickets.queue' y enviado a DLQ. motivo=El cuerpo del mensaje no es un PedidoEvento JSON valido
[DLQ] Mensaje muerto recibido. dlq=pedidos360.tickets.dlq colaOrigen=pedidos360.tickets.queue motivo=rejected
```

Para ver los mensajes guardados en la DLQ desde la consola, inicia el consumidor con
`DLQ_LISTENER_HABILITADO=false`.

**Reintentos.** Un fallo transitorio (por ejemplo, no poder escribir el ticket en disco) se registra como
`[REINTENTO] Intento 1/3 ...` y, si se agotan, el mensaje termina en la DLQ.

**Mensajeria desacoplada.** Deten `notificaciones-service` y crea pedidos: el backend responde igual y
los mensajes se acumulan en las colas. Al volver a iniciarlo, se procesan todos.

### Kafka

**Flujo normal.** Crea un pedido. En el mismo momento:

- `notificaciones-service` registra la notificacion y el ticket (RabbitMQ) y la linea de auditoria (Kafka)
  en `notificaciones-service/auditoria/pedidos-AAAA-MM-DD.txt`.
- En Kafka-UI (http://localhost:8090), *Topics* -> `pedidos360.pedidos.eventos` -> *Messages* muestra el
  evento con su clave (numero de pedido), particion y offset.
- `analitica-service` consume el evento y publica la foto `fecha#pN` en `pedidos360.metricas.ventas`.
- `GET /api/admin/metricas` en el backend (con token ADMIN) devuelve las metricas del dia sumadas.

**Consumer groups.** En Kafka-UI, *Consumers*: `pedidos360-analitica` y `pedidos360-auditoria` tienen 3
miembros cada uno (uno por particion) y lag 0. Los dos grupos reciben el mismo evento.

**Reintentos y DLT.** En Kafka-UI, *Topics* -> `pedidos360.pedidos.eventos` -> *Produce message* con un
valor que no sea JSON (por ejemplo `hola`) o un JSON sin numero de pedido. En el log de analitica-service
se ven `[KAFKA-DLT]` y el registro del `DeadLetterListener` con el grupo, el topico de origen y el motivo;
el mensaje queda en `pedidos360.pedidos.eventos.DLT`. Un error recuperable muestra antes
`[KAFKA-REINTENTO] intento 1/3 ...` con espera de 1 s, 2 s y 4 s.

**Reconstruccion del estado.** Deten `analitica-service`, crea pedidos y vuelve a iniciarlo: relee sus
particiones, procesa los pendientes y las metricas quedan iguales que si no se hubiera detenido. Lo mismo
con el backend: al iniciar, reconstruye la vista leyendo el topico compactado desde el inicio.

**Kafka desacoplado.** Con `KAFKA_ENABLED=false`, o con el cluster detenido, el backend guarda los pedidos
y RabbitMQ sigue funcionando; el error de Kafka solo queda en el log.

## API del microservicio administrador de RabbitMQ (`http://localhost:8081`)

Todas las rutas `/api/**` exigen `Authorization: Bearer <JWT>` de un usuario con App Role `ADMIN`.
Ejemplos listos para ejecutar en `rabbit-admin-service/requests.http`, y una demostracion completa
(creacion, validaciones y eliminacion) con `powershell -ExecutionPolicy Bypass -File .\demo.ps1`.

| Metodo | Ruta | Descripcion | Respuestas |
|---|---|---|---|
| `POST` | `/api/queues` | Crea una cola | `201`, `400`, `409` |
| `GET` | `/api/queues/{name}` | Mensajes pendientes y consumidores de una cola | `200`, `404` |
| `DELETE` | `/api/queues/{name}` | Elimina una cola | `204`, `404` |
| `POST` | `/api/exchanges` | Crea un exchange (`direct`, `topic`, `fanout`, `headers`) | `201`, `400`, `409` |
| `DELETE` | `/api/exchanges/{name}` | Elimina un exchange | `204`, `404` |
| `POST` | `/api/bindings` | Enlaza una cola con un exchange | `201`, `400`, `404` |
| `DELETE` | `/api/bindings?queue=&exchange=&routingKey=` | Elimina un binding | `204`, `400`, `404` |

Ademas responde `401` sin token, `403` sin rol ADMIN y `503` si RabbitMQ no esta disponible.

Ejemplo de cola con DLQ:

```json
{
  "name": "pedidos360.reportes.queue",
  "durable": true,
  "deadLetterExchange": "pedidos360.dlx",
  "deadLetterRoutingKey": "reportes.dlq",
  "messageTtl": 60000,
  "maxLength": 1000
}
```

Reglas de validacion: el nombre es obligatorio, de hasta 255 caracteres, solo con letras, numeros,
`.`, `-`, `_` y `:`, y no puede comenzar con `amq.` (reservado por RabbitMQ). `messageTtl` y `maxLength`
deben ser positivos y `deadLetterRoutingKey` exige indicar `deadLetterExchange`.

## API del microservicio administrador de Kafka (`http://localhost:8083`)

Misma seguridad que el administrador de RabbitMQ (JWT de Entra ID con App Role `ADMIN`). Ejemplos de
todas las llamadas en `kafka-admin-service/requests.http`.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/api/cluster` | Brokers, controlador e identificador del cluster |
| `GET` | `/api/topics` | Lista los topicos con particiones y replicas |
| `GET` | `/api/topics/{name}` | Detalle: particiones, lider, replicas, ISR y configuraciones |
| `POST` | `/api/topics` | Crea un topico con particiones, replicas y configuraciones |
| `PUT` | `/api/topics/{name}/configs` | Cambia configuraciones (retencion, cleanup, etc.) |
| `DELETE` | `/api/topics/{name}/configs/{key}` | Vuelve una configuracion a su valor por defecto |
| `PATCH` | `/api/topics/{name}/partitions` | Aumenta las particiones |
| `DELETE` | `/api/topics/{name}` | Elimina un topico |
| `GET` | `/api/consumer-groups` | Lista los consumer groups y su estado |
| `GET` | `/api/consumer-groups/{id}` | Miembros, particiones asignadas, offsets y lag |
| `POST` | `/api/consumer-groups/{id}/reset-offsets` | Mueve los offsets a `EARLIEST`, `LATEST` o un `OFFSET` dado |
| `DELETE` | `/api/consumer-groups/{id}` | Elimina un consumer group |

Respuestas: `200`/`201`/`204` si todo sale bien, `400` si la solicitud no es valida, `404` si el topico o
grupo no existe, `409` si ya existe o la operacion no esta permitida, `503` si el cluster no responde,
y `401`/`403` sin token o sin rol ADMIN.

Reglas que protegen el cluster:

- Solo se aceptan 13 configuraciones conocidas (`cleanup.policy`, `retention.ms`, `min.insync.replicas`,
  `compression.type`, etc.), con sus valores validados. `min.insync.replicas` no puede superar el factor de replica.
- Los topicos de Pedidos360 estan protegidos (`app.kafka-admin.topicos-protegidos`): no se pueden eliminar desde la API.
- Las particiones solo se pueden aumentar (Kafka no permite reducirlas) y hasta un maximo de 50.
- No se mueven los offsets de un grupo con consumidores activos.

Ejemplo de creacion de topico:

```json
{
  "name": "pedidos360.reportes",
  "partitions": 3,
  "replicationFactor": 3,
  "configs": {
    "cleanup.policy": "delete",
    "retention.ms": "86400000",
    "min.insync.replicas": "2"
  }
}
```

## Despliegue en la nube

| Componente | Donde corre |
|---|---|
| Frontend | GitHub Pages: https://sebaescobar4.github.io/Pedidos360-CloudNative/ |
| IDaaS | Microsoft Entra ID (registros `Pedidos360-SPA` y `Pedidos360-API`) |
| API publica (HTTPS) | AWS API Gateway (HTTP API) hacia la instancia EC2 |
| `backend` y `notificaciones-service` | AWS EC2 (Amazon Linux 2023), como servicios systemd |
| RabbitMQ | Contenedor Docker en la misma instancia EC2, accesible solo desde ella |
| Kafka, `analitica-service` y `kafka-admin-service` | Entorno local con Docker (un cluster de 3 brokers no cabe en la instancia EC2 actual). En EC2 se usa `KAFKA_ENABLED=false` |
| Base de datos | PostgreSQL en Supabase |

Cada push a `main` despliega automaticamente con GitHub Actions (`.github/workflows`). El workflow del
backend usa los secretos `EC2_HOST`, `EC2_USER` y `EC2_SSH_KEY` del repositorio.

La clave de la base de datos no esta en el codigo: se entrega con la variable de entorno
`SPRING_DATASOURCE_PASSWORD` (en el servidor, en `/etc/pedidos360.env`, junto con `KAFKA_ENABLED=false`).

## Pruebas

```bash
cd backend && ./mvnw test
cd notificaciones-service && ./mvnw test
cd analitica-service && ./mvnw test
cd rabbit-admin-service && ./mvnw test
cd kafka-admin-service && ./mvnw test
```

Estas pruebas no necesitan brokers en ejecucion: las de los administradores cubren seguridad (401/403),
validacion (400) y codigos de respuesta, y en el backend las pruebas corren con `app.kafka.enabled=false`.

Pruebas de integracion contra el cluster Kafka real (con el cluster levantado):

```bash
cd backend && KAFKA_IT=true ./mvnw test -Dtest=PedidoKafkaIntegrationTest
cd kafka-admin-service && KAFKA_IT=true ./mvnw test -Dtest=KafkaAdminIntegrationTest
```

- `PedidoKafkaIntegrationTest` publica un evento y lo lee del topico. Con `KAFKA_IT_ANALITICA=true` y
  analitica-service corriendo, verifica ademas el circuito completo hasta las metricas.
- `KafkaAdminIntegrationTest` ejecuta 22 llamadas a la API (topicos y consumer groups) y deja el registro en
  `target/kafka-admin-demo.txt`. Con `KAFKA_IT_CONSERVAR=true` no borra el topico de prueba.
