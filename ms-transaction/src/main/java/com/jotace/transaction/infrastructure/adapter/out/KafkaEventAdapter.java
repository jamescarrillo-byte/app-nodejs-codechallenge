package com.jotace.transaction.infrastructure.adapter.out;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jotace.transaction.application.port.out.EventProducerPort;
import com.jotace.transaction.domain.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventAdapter implements EventProducerPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventAdapter.class);
    private static final String TOPIC = "transaction-created";

    private final ObjectMapper MAPPER;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventAdapter(ObjectMapper MAPPER, KafkaTemplate<String, String> kafkaTemplate) {
        this.MAPPER = MAPPER;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendTransactionCreatedEvent(Transaction transaction) {
        String message = convertTransactionToJson(transaction);
        try {
            kafkaTemplate.send(TOPIC, transaction.transactionExternalId().toString(), message);
            log.info("Event published to Kafka TOPIC: {} | Key: {} | Status: {}",
                    TOPIC,
                    transaction.transactionExternalId(),
                    transaction.transactionStatus());
        } catch (Exception e) {
            log.error("Failed to send transaction event to Kafka for ID: {}",
                    transaction.transactionExternalId(), e);
            throw new IllegalStateException("Kafka publishing failed for transaction.", e);
        }
    }

    // Converts the Transaction domain object to JSON. Catches and wraps the checked JsonProcessingException.
    private String convertTransactionToJson(Transaction transaction) {
        try {
            return MAPPER.writeValueAsString(transaction);
        } catch (Exception e) {
            log.error("Error serializing transaction {} to JSON for Kafka.", transaction.transactionExternalId(), e);
            throw new IllegalStateException("Failed to convert Transaction to JSON.", e);
        }
    }
}