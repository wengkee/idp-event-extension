package com.wengkee.idp.event.notification;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Arrays;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOGGER = Logger.getLogger(KafkaEventListenerProviderFactory.class);
    private static final String ID = "kafka_event_listener";

    private KafkaEventListenerProvider kafkaEventListenerProvider;

    private String kafkaBootstrapServers;
    private String kafkaClientID;
    private String kafkaTopic;
    private String[] keycloakEventTypes;

    @Override
    public EventListenerProvider create(KeycloakSession keycloakSession) {
        if (kafkaEventListenerProvider == null){
            kafkaEventListenerProvider = new KafkaEventListenerProvider(kafkaBootstrapServers, kafkaClientID, kafkaTopic, keycloakEventTypes);
        }
        return kafkaEventListenerProvider;
    }

    @Override
    public void init(Config.Scope scope) {

        kafkaTopic = scope.get("KAFKA_TOPIC", System.getenv("KAFKA_TOPIC"));
        kafkaClientID = scope.get("KAFKA_CLIENT_ID", System.getenv("KAFKA_CLIENT_ID"));
        kafkaBootstrapServers = scope.get("KAFKA_BOOTSTRAP_SERVERS", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));

        String eventTypesString = scope.get("KEYCLOAK_EVENT_TYPES", System.getenv("KEYCLOAK_EVENT_TYPES"));
        if (eventTypesString != null){
            keycloakEventTypes = eventTypesString.split(",");
        } else {
            keycloakEventTypes = new String[1];
            keycloakEventTypes[0] = "LOGIN";
        }

        LOGGER.info("Initializing Kafka Event Listener..");
        LOGGER.info("KAFKA_TOPIC: [" + kafkaTopic + "]");
        LOGGER.info("KAFKA_CLIENT_ID: [" + kafkaClientID + "]");
        LOGGER.info("KAFKA_BOOTSTRAP_SERVERS: [" + kafkaBootstrapServers + "]");
        LOGGER.info("KEYCLOAK_EVENT_TYPES: [" + Arrays.toString(keycloakEventTypes) + "]");

    }

    @Override
    public void postInit(KeycloakSessionFactory keycloakSessionFactory) {}

    @Override
    public void close() {}

    @Override
    public String getId() {
        return ID;
    }
}
