package br.com.microservices.choreography.paymentservice.core.saga;

import br.com.microservices.choreography.paymentservice.core.dto.Event;
import br.com.microservices.choreography.paymentservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.paymentservice.core.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SagaExecutionController {

    @Value("${spring.kafka.topic.inventory-success}")
    private String inventorySuccessTopic;

    @Value("${spring.kafka.topic.payment-fail}")
    private String paymentFailTopic;

    @Value("${spring.kafka.topic.product-validation-fail}")
    private String productValidationFailTopic;

    private final JsonUtil jsonUtil;
    private final KafkaProducer producer;

    public SagaExecutionController(JsonUtil jsonUtil, KafkaProducer producer) {
        this.jsonUtil = jsonUtil;
        this.producer = producer;
    }

    public void handleSaga(Event event) {
        switch (event.getStatus()) {
            case SUCCESS -> handleSuccess(event);
            case ROLLBACK_PENDING -> handleRollbackPending(event);
            case FAIL -> handleFail(event);
        }
    }

    private void handleSuccess(Event event) {
        log.info("### CURRENT SAGA: {} | SUCCESS | NEXT TOPIC {} | {}",
                event.getSource(), inventorySuccessTopic, event.getSagaId());

        sendEvent(event, inventorySuccessTopic);
    }

    private void handleRollbackPending(Event event) {
        log.info("### CURRENT SAGA: {} | SENDING TO ROLLBACK CURRENT SERVICE | NEXT TOPIC {} | {}",
                event.getSource(), paymentFailTopic, event.getSagaId());

        sendEvent(event, paymentFailTopic);
    }

    private void handleFail(Event event) {
        log.info("### CURRENT SAGA: {} | FAIL | NEXT TOPIC {} | {}",
                event.getSource(), productValidationFailTopic, event.getSagaId());

        sendEvent(event, productValidationFailTopic);
    }

    private void sendEvent(Event event, String topic) {
        String json = jsonUtil.toJson(event);
        producer.sendEvent(json, topic);
    }
}
