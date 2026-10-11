package cl.duoc.pedidos360.rabbitadmin.dto;

/**
 * Reglas compartidas por los DTO para validar nombres de recursos de RabbitMQ.
 */
public final class NombresRabbit {

    // Letras, numeros, punto, guion, guion bajo y dos puntos. El prefijo 'amq.' esta reservado por RabbitMQ.
    public static final String PATRON = "^(?!amq\\.)[A-Za-z0-9._:-]+$";

    public static final String MENSAJE_PATRON =
            "solo puede contener letras, numeros, '.', '-', '_' y ':' y no puede comenzar con 'amq.'";

    public static final int LARGO_MAXIMO = 255;

    private NombresRabbit() {}
}
