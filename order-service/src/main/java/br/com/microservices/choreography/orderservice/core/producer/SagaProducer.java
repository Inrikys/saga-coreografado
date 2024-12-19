package br.com.microservices.choreography.orderservice.core.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SagaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic.product-validation-start}")
    private String productValidationStartTopic;

    public SagaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendEvent(String payload) {
        try {
            log.info("Sending event to topic {} with data {}", productValidationStartTopic, payload);
            kafkaTemplate.send(productValidationStartTopic, payload);
        } catch (Exception ex) {
            log.error("Error trying to send data to topic {} with data {}", productValidationStartTopic, payload, ex);
        }
    }
}
