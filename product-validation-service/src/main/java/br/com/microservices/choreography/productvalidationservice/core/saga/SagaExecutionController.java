package br.com.microservices.choreography.productvalidationservice.core.saga;

import br.com.microservices.choreography.productvalidationservice.core.dto.Event;
import br.com.microservices.choreography.productvalidationservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.productvalidationservice.core.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static java.lang.String.format;

@Slf4j
@Component
public class SagaExecutionController {

    @Value("${spring.kafka.topic.payment-success}")
    private String paymentSuccessTopic;

    @Value("${spring.kafka.topic.product-validation-fail}")
    private String productValidationFailTopic;

    @Value("${spring.kafka.topic.notify-ending}")
    private String notifyEndingTopic;

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
                event.getSource(), paymentSuccessTopic, event.getSagaId());

        sendEvent(event, paymentSuccessTopic);
    }

    private void handleRollbackPending(Event event) {
        log.info("### CURRENT SAGA: {} | SENDING TO ROLLBACK CURRENT SERVICE | NEXT TOPIC {} | {}",
                event.getSource(), productValidationFailTopic, event.getSagaId());

        sendEvent(event, productValidationFailTopic);
    }

    private void handleFail(Event event) {
        log.info("### CURRENT SAGA: {} | FAIL | NEXT TOPIC {} | {}",
                event.getSource(), notifyEndingTopic, event.getSagaId());

        sendEvent(event, notifyEndingTopic);
    }

    private void sendEvent(Event event, String topic) {
        String json = jsonUtil.toJson(event);
        producer.sendEvent(json, topic);
    }
}
