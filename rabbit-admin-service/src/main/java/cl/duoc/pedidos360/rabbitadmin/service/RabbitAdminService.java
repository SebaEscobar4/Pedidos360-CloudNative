package cl.duoc.pedidos360.rabbitadmin.service;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeResponse;
import cl.duoc.pedidos360.rabbitadmin.dto.NombresRabbit;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueResponse;
import cl.duoc.pedidos360.rabbitadmin.exception.NombreInvalidoException;
import cl.duoc.pedidos360.rabbitadmin.exception.RecursoNoEncontradoException;
import cl.duoc.pedidos360.rabbitadmin.exception.RecursoYaExisteException;

/**
 * =========================================================================
 * SERVICIO: RabbitAdminService
 * =========================================================================
 * FUNCION: Encapsula TODA la interaccion con RabbitMQ para administrar su topologia.
 *          Los controladores solo llaman a estos metodos de alto nivel y no conocen
 *          AmqpAdmin, Queue, Exchange ni Binding.
 *
 *          Colas     : crearCola / obtenerCola / eliminarCola
 *          Exchanges : crearExchange / eliminarExchange
 *          Bindings  : crearBinding / eliminarBinding
 *
 * CONECTA CON: QueueController, ExchangeController, BindingController.
 */
@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    private static final Pattern NOMBRE_VALIDO = Pattern.compile(NombresRabbit.PATRON);

    private final AmqpAdmin amqpAdmin;
    private final RabbitTemplate rabbitTemplate;

    public RabbitAdminService(AmqpAdmin amqpAdmin, RabbitTemplate rabbitTemplate) {
        this.amqpAdmin = amqpAdmin;
        this.rabbitTemplate = rabbitTemplate;
    }

    // -------------------------------------------------------------------- Colas

    public QueueResponse crearCola(QueueRequest request) {
        if (amqpAdmin.getQueueInfo(request.name()) != null) {
            throw new RecursoYaExisteException("Ya existe una cola llamada '" + request.name() + "'");
        }

        QueueBuilder builder = request.durableOrDefault()
                ? QueueBuilder.durable(request.name())
                : QueueBuilder.nonDurable(request.name());
        if (tieneTexto(request.deadLetterExchange())) {
            builder.deadLetterExchange(request.deadLetterExchange());
        }
        if (tieneTexto(request.deadLetterRoutingKey())) {
            builder.deadLetterRoutingKey(request.deadLetterRoutingKey());
        }
        if (request.messageTtl() != null) {
            builder.withArgument("x-message-ttl", request.messageTtl());
        }
        if (request.maxLength() != null) {
            builder.maxLength(request.maxLength());
        }

        Queue cola = builder.build();
        amqpAdmin.declareQueue(cola);
        log.info("[RABBIT-ADMIN] Cola creada: {} argumentos={}", cola.getName(), cola.getArguments());
        return new QueueResponse(cola.getName(), 0, 0);
    }

    public QueueResponse obtenerCola(String nombre) {
        validarNombre(nombre);
        QueueInformation info = amqpAdmin.getQueueInfo(nombre);
        if (info == null) {
            throw new RecursoNoEncontradoException("No existe la cola '" + nombre + "'");
        }
        return new QueueResponse(info.getName(), info.getMessageCount(), info.getConsumerCount());
    }

    public void eliminarCola(String nombre) {
        obtenerCola(nombre);
        amqpAdmin.deleteQueue(nombre);
        log.info("[RABBIT-ADMIN] Cola eliminada: {}", nombre);
    }

    // ---------------------------------------------------------------- Exchanges

    public ExchangeResponse crearExchange(ExchangeRequest request) {
        if (existeExchange(request.name())) {
            throw new RecursoYaExisteException("Ya existe un exchange llamado '" + request.name() + "'");
        }
        Exchange exchange = new ExchangeBuilder(request.name(), request.type())
                .durable(request.durableOrDefault())
                .build();
        amqpAdmin.declareExchange(exchange);
        log.info("[RABBIT-ADMIN] Exchange creado: {} tipo={}", exchange.getName(), exchange.getType());
        return new ExchangeResponse(exchange.getName(), exchange.getType(), exchange.isDurable());
    }

    public void eliminarExchange(String nombre) {
        validarNombre(nombre);
        exigirExchange(nombre);
        amqpAdmin.deleteExchange(nombre);
        log.info("[RABBIT-ADMIN] Exchange eliminado: {}", nombre);
    }

    // ----------------------------------------------------------------- Bindings

    public BindingRequest crearBinding(BindingRequest request) {
        obtenerCola(request.queue());
        exigirExchange(request.exchange());
        amqpAdmin.declareBinding(aBinding(request));
        log.info("[RABBIT-ADMIN] Binding creado: {} --({})--> {}",
                request.exchange(), request.routingKey(), request.queue());
        return request;
    }

    public void eliminarBinding(BindingRequest request) {
        obtenerCola(request.queue());
        exigirExchange(request.exchange());
        amqpAdmin.removeBinding(aBinding(request));
        log.info("[RABBIT-ADMIN] Binding eliminado: {} --({})--> {}",
                request.exchange(), request.routingKey(), request.queue());
    }

    // ------------------------------------------------------------------ Apoyo

    private Binding aBinding(BindingRequest request) {
        return new Binding(request.queue(), DestinationType.QUEUE, request.exchange(), request.routingKey(), null);
    }

    private void exigirExchange(String nombre) {
        if (!existeExchange(nombre)) {
            throw new RecursoNoEncontradoException("No existe el exchange '" + nombre + "'");
        }
    }

    // AmqpAdmin no ofrece consulta de exchanges: se usa una declaracion pasiva, que falla si no existe
    private boolean existeExchange(String nombre) {
        try {
            rabbitTemplate.execute(channel -> channel.exchangeDeclarePassive(nombre));
            return true;
        } catch (AmqpConnectException e) {
            throw e;
        } catch (AmqpException e) {
            return false;
        }
    }

    // Los nombres que llegan por la URL no pasan por un DTO: se validan aqui con la misma regla
    private void validarNombre(String nombre) {
        if (nombre == null || nombre.length() > NombresRabbit.LARGO_MAXIMO || !NOMBRE_VALIDO.matcher(nombre).matches()) {
            throw new NombreInvalidoException("El nombre " + NombresRabbit.MENSAJE_PATRON);
        }
    }

    private boolean tieneTexto(String valor) {
        return valor != null && !valor.isBlank();
    }
}
