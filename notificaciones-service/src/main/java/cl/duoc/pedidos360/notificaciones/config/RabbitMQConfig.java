package cl.duoc.pedidos360.notificaciones.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * =========================================================================
 * CONFIGURACION: RabbitMQConfig
 * =========================================================================
 * FUNCION: Declara como beans la topologia que este microservicio consume.
 *          Es identica a la del backend de pedidos, asi cualquiera de los dos
 *          puede iniciar primero y dejar el broker correctamente configurado.
 *
 *  CASO DE USO 1 - Notificaciones al cliente
 *    pedidos.exchange --(pedido.#)--> notificaciones.queue --(falla)--> dlx --> notificaciones.dlq
 *
 *  CASO DE USO 2 - Generacion de tickets de compra
 *    pedidos.exchange --(pedido.creado)--> tickets.queue --(falla)--> dlx --> tickets.dlq
 *
 * CONECTA CON: RabbitMQProperties (nombres) y los consumidores @RabbitListener.
 */
@Configuration
@EnableConfigurationProperties(RabbitMQProperties.class)
public class RabbitMQConfig {

    private final RabbitMQProperties props;

    public RabbitMQConfig(RabbitMQProperties props) {
        this.props = props;
    }

    // ---------------------------------------------------------------- Exchanges

    @Bean
    TopicExchange pedidosExchange() {
        return new TopicExchange(props.exchanges().pedidos(), true, false);
    }

    // Dead Letter Exchange: recibe los mensajes que los consumidores rechazan sin requeue
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
}
