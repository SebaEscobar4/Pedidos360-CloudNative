# Pedidos360 - Desarrollo Cloud Native I (DSY1107)

Sistema de pedidos de comida. **Evaluacion Parcial N° 3:** se incorpora RabbitMQ para procesar
de forma asincrona las notificaciones al cliente y la generacion de tickets, sin modificar la
logica existente del sistema.

## Componentes

| Carpeta | Que es | Puerto | Rol en la mensajeria |
|---|---|---|---|
| `frontend/` | SPA Angular con login en Microsoft Entra ID (IDaaS) | 4200 | - |
| `backend/` | Microservicio de pedidos (Spring Boot + JPA + JWT) | 8080 | **Productor** de eventos de pedidos |
| `notificaciones-service/` | Microservicio de notificaciones y tickets | 8082 | **Consumidor** con ACK manual, reintentos y DLQ |
| `rabbit-admin-service/` | Microservicio administrador de RabbitMQ (protegido con JWT, rol ADMIN) | 8081 | API REST de colas, exchanges y bindings |
| `docker-compose.yml` | Broker RabbitMQ con consola de administracion | 5672 / 15672 | - |

## Flujo de mensajeria

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

## Donde esta cada punto de la pauta

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

## Como ejecutar

Requisitos: Java 21, Docker, Node 20+ (para el frontend).

**1. Levantar RabbitMQ**

```bash
docker compose up -d
```

Consola: http://localhost:15672 (usuario `guest`, clave `guest`).

**2. Levantar los microservicios** (cada uno en su propia terminal)

```bash
cd backend && ./mvnw spring-boot:run
```

```bash
cd notificaciones-service && ./mvnw spring-boot:run
```

```bash
cd rabbit-admin-service && ./mvnw spring-boot:run
```

**3. Levantar el frontend**

```bash
cd frontend && npm install && npm start
```

### Variables de entorno de RabbitMQ (los tres microservicios)

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

## Como demostrar que funciona

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

## API del microservicio administrador (`http://localhost:8081`)

Todas las rutas `/api/**` exigen `Authorization: Bearer <JWT>` de un usuario con App Role `ADMIN`.
Ejemplos listos para ejecutar en `rabbit-admin-service/requests.http`.

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

## Pruebas

```bash
cd backend && ./mvnw test
```

```bash
cd rabbit-admin-service && ./mvnw test
```

Las pruebas de `rabbit-admin-service` cubren seguridad (401/403), validacion (400) y codigos de
respuesta (201/204/404/409) y no necesitan un broker en ejecucion.
