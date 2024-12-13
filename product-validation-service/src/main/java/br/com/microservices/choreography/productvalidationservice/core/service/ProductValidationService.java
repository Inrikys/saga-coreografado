package br.com.microservices.choreography.productvalidationservice.core.service;

import br.com.microservices.choreography.productvalidationservice.core.dto.Event;
import br.com.microservices.choreography.productvalidationservice.core.model.Validation;
import br.com.microservices.choreography.productvalidationservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.productvalidationservice.core.repository.ProductRepository;
import br.com.microservices.choreography.productvalidationservice.core.repository.ValidationRepository;
import br.com.microservices.choreography.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class ProductValidationService {

    private static final String CURRENT_SOURCE = "PRODUCT_VALIDATION_SERVICE";

    private final JsonUtil jsonUtil;
    private final KafkaProducer producer;
    private final ProductRepository productRepository;
    private final ValidationRepository validationRepository;

    public void validateExistingProducts(Event event) {
        try {
            event.validate(validationRepository, productRepository);
            createValidation(event, true);
            event.addHistorySuccess(CURRENT_SOURCE);
        } catch (Exception ex) {
            log.error("Error trying to validate products: ", ex);
            event.addHistoryFail(CURRENT_SOURCE, ex.getMessage());
        }

        producer.sendEvent(jsonUtil.toJson(event), "ALTERAR");
    }

    public void rollbackEvent(Event event) {
        changeValidationToFail(event);
        event.addHistoryRollback(CURRENT_SOURCE);

        producer.sendEvent(jsonUtil.toJson(event), "ALTERAR");
    }

    private void changeValidationToFail(Event event) {
        validationRepository
                .findByOrderIdAndTransactionId(event.getPayload().getId(), event.getPayload().getTransactionId())
                .ifPresentOrElse(validation -> {
                    validation.setSuccess(false);
                    validationRepository.save(validation);
                }, () -> createValidation(event, false));
    }

    private void createValidation(Event event, boolean success) {
        Validation validation = Validation.builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .success(success)
                .build();

        validationRepository.save(validation);
    }
}
