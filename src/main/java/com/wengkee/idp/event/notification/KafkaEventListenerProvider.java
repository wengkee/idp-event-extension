package com.wengkee.idp.event.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaEventListenerProvider implements EventListenerProvider{

    private static final Logger LOGGER = Logger.getLogger(KafkaEventListenerProvider.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String kafkaTopic;
    private List<EventType> keycloakEventTypes;
    private Producer<String, String> kafkaProducer;

    public KafkaEventListenerProvider(String kafkaBootstrapServers, String kafkaClientID, String kafkaTopic, String[] keycloakEventTypes){

        this.kafkaTopic = kafkaTopic;
        this.keycloakEventTypes = new ArrayList<>();

        for ( String keycloakEvent : keycloakEventTypes ){
            try {
                EventType eventType = EventType.valueOf(keycloakEvent.toUpperCase());
                this.keycloakEventTypes.add(eventType);
            } catch (IllegalArgumentException e){
                LOGGER.debug("Event: [" + keycloakEvent + "] doesn't exists.");
            }
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public void onEvent(Event event) {
        if (keycloakEventTypes.contains(event.getType())) {
            dispatchEvent(event);
        }
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean b) {
//        if (adminEvent != null) {
//            dispatchEvent(adminEvent);
//        }
    }

    @Override
    public void close() {}

    private void dispatchEvent(Object event)  {
        try {

            String json = MAPPER.writeValueAsString(event);
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, json));

        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
