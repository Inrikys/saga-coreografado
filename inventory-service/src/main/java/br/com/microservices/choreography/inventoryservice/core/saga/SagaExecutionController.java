package br.com.microservices.choreography.inventoryservice.core.saga;

import br.com.microservices.choreography.inventoryservice.core.dto.Event;
import br.com.microservices.choreography.inventoryservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.inventoryservice.core.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SagaExecutionController {

    @Value("${spring.kafka.topic.notify-ending}")
    private String notifyEndingTopic;

    @Value("${spring.kafka.topic.inventory-fail}")
    private String inventoryFailTopic;

    @Value("${spring.kafka.topic.payment-fail}")
    private String paymentFailTopic;

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
                event.getSource(), notifyEndingTopic, event.getSagaId());

        sendEvent(event, notifyEndingTopic);
    }

    private void handleRollbackPending(Event event) {
        log.info("### CURRENT SAGA: {} | SENDING TO ROLLBACK CURRENT SERVICE | NEXT TOPIC {} | {}",
                event.getSource(), inventoryFailTopic, event.getSagaId());

        sendEvent(event, inventoryFailTopic);
    }

    private void handleFail(Event event) {
        log.info("### CURRENT SAGA: {} | FAIL | NEXT TOPIC {} | {}",
                event.getSource(), paymentFailTopic, event.getSagaId());

        sendEvent(event, paymentFailTopic);
    }

    private void sendEvent(Event event, String topic) {
        String json = jsonUtil.toJson(event);
        producer.sendEvent(json, topic);
    }
}
