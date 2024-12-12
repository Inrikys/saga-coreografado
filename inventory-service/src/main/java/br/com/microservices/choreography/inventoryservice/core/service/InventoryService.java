package br.com.microservices.choreography.inventoryservice.core.service;

import br.com.microservices.choreography.inventoryservice.config.exception.ValidationException;
import br.com.microservices.choreography.inventoryservice.core.dto.Event;
import br.com.microservices.choreography.inventoryservice.core.dto.Order;
import br.com.microservices.choreography.inventoryservice.core.dto.OrderProducts;
import br.com.microservices.choreography.inventoryservice.core.enums.ESagaStatus;
import br.com.microservices.choreography.inventoryservice.core.model.Inventory;
import br.com.microservices.choreography.inventoryservice.core.model.OrderInventory;
import br.com.microservices.choreography.inventoryservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.inventoryservice.core.repository.InventoryRepository;
import br.com.microservices.choreography.inventoryservice.core.repository.OrderInvetoryRepository;
import br.com.microservices.choreography.inventoryservice.core.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryService {

    private static final String CURRENT_SOURCE = "INVENTORY_SERVICE";

    private final JsonUtil jsonUtil;
    private final KafkaProducer producer;
    private final InventoryRepository inventoryRepository;
    private final OrderInvetoryRepository orderInventoryRepository;

    public InventoryService(JsonUtil jsonUtil, KafkaProducer producer, InventoryRepository inventoryRepository, OrderInvetoryRepository orderInventoryRepository) {
        this.jsonUtil = jsonUtil;
        this.producer = producer;
        this.inventoryRepository = inventoryRepository;
        this.orderInventoryRepository = orderInventoryRepository;
    }

    public void updateInventory(Event event) {
        try {
            checkCurrentValidation(event);
            createOrderInventory(event);
            updateInventory(event.getPayload());
            event.addHistorySuccess(CURRENT_SOURCE);
        } catch (Exception ex) {
            log.error("Error trying to update inventory: ", ex);
            event.addHistoryFail(ex.getMessage(), CURRENT_SOURCE);
        }
    }

    private void updateInventory(Order order) {
        order.getProducts()
                .forEach(product -> {
                    Inventory inventory = findInventoryByProductCode(product.getProduct().getCode());
                    checkInventory(inventory.getAvailable(), product.getQuantity());
                    inventory.setAvailable(inventory.getAvailable() - product.getQuantity());
                });
    }

    private void checkInventory(int available, int orderQuantity) {
        if (orderQuantity > available) {
            throw new ValidationException("Product is out of stock!");
        }
    }

    private void checkCurrentValidation(Event event) {
        if (orderInventoryRepository.existsByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())) {
            throw new ValidationException("There's another transactionId for this validation.");
        }
    }

    private void createOrderInventory(Event event) {
        event.getPayload()
                .getProducts()
                .forEach(product -> {
                    Inventory inventoryByProductCode = findInventoryByProductCode(product.getProduct().getCode());
                    OrderInventory orderInventory = createOrderInventory(event, product, inventoryByProductCode);

                    orderInventoryRepository.save(orderInventory);
                });
    }

    private OrderInventory createOrderInventory(Event event, OrderProducts product, Inventory inventory) {
        return OrderInventory.builder()
                .inventory(inventory)
                .oldQuantity(inventory.getAvailable())
                .orderQuantity(product.getQuantity())
                .newQuantity(inventory.getAvailable() - product.getQuantity())
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .build();
    }

    private Inventory findInventoryByProductCode(String productCode) {
        return inventoryRepository
                .findByProductCode(productCode)
                .orElseThrow(() -> new ValidationException("Inventory not found by informed product"));
    }

    public void rollbackInventory(Event event) {
        event.setStatus(ESagaStatus.FAIL);
        event.setSource(CURRENT_SOURCE);
        try {
            returnInventoryToPreviousValues(event);
            event.addHistory(event, "Rollback executed for inventory!");
        } catch (Exception ex) {
            event.addHistoryFail("- Inventory failed: ".concat(ex.getMessage()), CURRENT_SOURCE);
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void returnInventoryToPreviousValues(Event event) {
        orderInventoryRepository
                .findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                .forEach(orderInventory -> {
                    Inventory inventory = orderInventory.getInventory();
                    inventory.setAvailable(orderInventory.getOldQuantity());
                    inventoryRepository.save(inventory);
                    log.info("Restored inventory for order {} from {} to {}",
                            event.getPayload().getId(), orderInventory.getNewQuantity(), inventory.getAvailable());
                });
    }
}
