package br.com.microservices.choreography.orderservice.core.service;

import br.com.microservices.choreography.orderservice.config.exception.ValidationException;
import br.com.microservices.choreography.orderservice.core.document.Event;
import br.com.microservices.choreography.orderservice.core.document.Order;
import br.com.microservices.choreography.orderservice.core.dto.EventFilters;
import br.com.microservices.choreography.orderservice.core.enums.ESagaStatus;
import br.com.microservices.choreography.orderservice.core.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static br.com.microservices.choreography.orderservice.core.enums.ESagaStatus.*;
import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {

    private final EventRepository repository;

    private static final String CURRENT_SERVICE = "ORDER_SERVICE";

    public Event save(Event event) {
        return repository.save(event);
    }

    public void notifyEnding(Event event) {
        event.setSource(CURRENT_SERVICE);
        event.setOrderId(event.getOrderId());
        event.setCreatedAt(LocalDateTime.now());

        setEndingHistory(event);
        save(event);

        log.info("Order {} with saga notified! TransactionId: {}", event.getOrderId(), event.getTransactionId());
    }

    public List<Event> findAll() {
        return repository.findAllByOrderByCreatedAtDesc();
    }

    public Event findByFilters(EventFilters filters) {
        filters.isValid();

        if (!isEmpty(filters.getOrderId())) {
            return repository.findTop1ByOrderIdOrderByCreatedAtDesc(filters.getOrderId())
                    .orElseThrow(() -> new ValidationException("Event not found by orderId."));
        } else {
            return repository.findTop1ByTransactionIdOrderByCreatedAtDesc(filters.getTransactionId())
                    .orElseThrow(() -> new ValidationException("Event not found by transactionId."));
        }
    }

    public Event createEvent(Order order) {
        var event = Event.builder()
                .source(CURRENT_SERVICE)
                .status(SUCCESS)
                .orderId(order.getId())
                .transactionId(order.getTransactionId())
                .payload(order)
                .createdAt(LocalDateTime.now())
                .build();

        event.addHistory("Saga started!");
        return save(event);
    }

    public void setEndingHistory(Event event) {
        if (ESagaStatus.SUCCESS.equals(event.getStatus())) {
            log.info("SAGA FINISHED SUCCESSFULLY FOR EVENT {}", event.getId());
            event.addHistory("Saga finished successfully!");
        } else {
            log.info("SAGA FINISHED WITH ERRORS FOR EVENT {}", event.getId());
            event.addHistory("Saga finished with errors!");
        }
    }

}
