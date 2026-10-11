package cl.duoc.pedidos360.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * =========================================================================
 * CONFIGURACION: RabbitMQConfig
 * =========================================================================
 * FUNCION: Declara como beans toda la topologia de mensajeria del dominio Pedidos.
 *          Al conectarse al broker, Spring crea automaticamente lo que no exista.
 *
 *  CASO DE USO 1 - Notificaciones al cliente
 *    pedidos.exchange --(pedido.#)--> notificaciones.queue --(falla)--> dlx --> notificaciones.dlq
 *
 *  CASO DE USO 2 - Generacion de tickets de compra
 *    pedidos.exchange --(pedido.creado)--> tickets.queue --(falla)--> dlx --> tickets.dlq
 *
 * CONECTA CON: RabbitMQProperties (nombres) y PedidoEventPublisher (usa el RabbitTemplate).
 */
@Configuration
@EnableConfigurationProperties(RabbitMQProperties.class)
public class RabbitMQConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQConfig.class);

    private final RabbitMQProperties props;

    public RabbitMQConfig(RabbitMQProperties props) {
        this.props = props;
    }

    // ---------------------------------------------------------------- Exchanges

    // Exchange principal: recibe todos los eventos de pedidos y los enruta por routing key
    @Bean
    TopicExchange pedidosExchange() {
        return new TopicExchange(props.exchanges().pedidos(), true, false);
    }

    // Dead Letter Exchange: recibe los mensajes rechazados por los consumidores
    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(props.exchanges().deadLetter(), true, false);
    }

    // ------------------------------------------- Caso de uso 1: Notificaciones

    @Bean
    Queue notificacionesQueue() {
        return QueueBuilder.durable(props.queues().notificaciones())
                .deadLetterExchange(props.exchanges().deadLetter())
                .deadLetterRoutingKey(props.routingKeys().notificacionesDlq())
                .build();
    }

    @Bean
    Queue notificacionesDlq() {
        return QueueBuilder.durable(props.queues().notificacionesDlq()).build();
    }

    @Bean
    Binding notificacionesBinding() {
        return BindingBuilder.bind(notificacionesQueue())
                .to(pedidosExchange())
                .with(props.routingKeys().notificacionesBinding());
    }

    @Bean
    Binding notificacionesDlqBinding() {
        return BindingBuilder.bind(notificacionesDlq())
                .to(deadLetterExchange())
                .with(props.routingKeys().notificacionesDlq());
    }

    // -------------------------------------------------- Caso de uso 2: Tickets

    @Bean
    Queue ticketsQueue() {
        return QueueBuilder.durable(props.queues().tickets())
                .deadLetterExchange(props.exchanges().deadLetter())
                .deadLetterRoutingKey(props.routingKeys().ticketsDlq())
                .build();
    }

    @Bean
    Queue ticketsDlq() {
        return QueueBuilder.durable(props.queues().ticketsDlq()).build();
    }

    @Bean
    Binding ticketsBinding() {
        return BindingBuilder.bind(ticketsQueue())
                .to(pedidosExchange())
                .with(props.routingKeys().ticketsBinding());
    }

    @Bean
    Binding ticketsDlqBinding() {
        return BindingBuilder.bind(ticketsDlq())
                .to(deadLetterExchange())
                .with(props.routingKeys().ticketsDlq());
    }

    // ------------------------------------------------- Infraestructura comun

    // Los eventos viajan como JSON para que cualquier microservicio pueda leerlos
    @Bean
    MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        template.setMandatory(true);
        // Mensajes que el broker no pudo enrutar a ninguna cola: se registran en el log
        template.setReturnsCallback(returned -> log.error(
                "[RABBIT] Mensaje NO ENRUTADO. exchange={} routingKey={} motivo={} cuerpo={}",
                returned.getExchange(),
                returned.getRoutingKey(),
                returned.getReplyText(),
                new String(returned.getMessage().getBody())));
        return template;
    }
}
