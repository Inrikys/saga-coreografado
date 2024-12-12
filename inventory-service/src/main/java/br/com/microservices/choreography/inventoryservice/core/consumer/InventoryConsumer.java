package br.com.microservices.choreography.inventoryservice.core.consumer;

import br.com.microservices.choreography.inventoryservice.core.service.InventoryService;
import br.com.microservices.choreography.inventoryservice.core.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InventoryConsumer {

    private final JsonUtil jsonUtil;
    private final InventoryService inventoryService;

    public InventoryConsumer(JsonUtil jsonUtil, InventoryService inventoryService) {
        this.jsonUtil = jsonUtil;
        this.inventoryService = inventoryService;
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.inventory-success}"
    )
    public void consumeSuccessEvent(String payload) {
        log.info("Receiving success event {} from inventory-success topic", payload);
        var event = jsonUtil.toEvent(payload);
        inventoryService.updateInventory(event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.inventory-fail}"
    )
    public void consumeFailEvent(String payload) {
        log.info("Receiving rollback event {} from inventory-success topic", payload);
        var event = jsonUtil.toEvent(payload);
        inventoryService.rollbackInventory(event);
    }
}
