package br.com.microservices.choreography.orderservice.core.service;

import br.com.microservices.choreography.orderservice.config.exception.ValidationException;
import br.com.microservices.choreography.orderservice.core.document.Event;
import br.com.microservices.choreography.orderservice.core.dto.EventFilters;
import br.com.microservices.choreography.orderservice.core.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {

    private final EventRepository repository;

    public Event save(Event event) {
        return repository.save(event);
    }

    public void notifyEnding(Event event) {

        event.setOrderId(event.getOrderId());
        event.setCreatedAt(LocalDateTime.now());
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
}
