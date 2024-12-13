package br.com.microservices.choreography.paymentservice.core.service;

import br.com.microservices.choreography.paymentservice.config.exception.ValidationException;
import br.com.microservices.choreography.paymentservice.core.dto.Event;
import br.com.microservices.choreography.paymentservice.core.enums.EPaymentStatus;
import br.com.microservices.choreography.paymentservice.core.enums.ESagaStatus;
import br.com.microservices.choreography.paymentservice.core.model.Payment;
import br.com.microservices.choreography.paymentservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.paymentservice.core.repository.PaymentRepository;
import br.com.microservices.choreography.paymentservice.core.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentService {

    private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";

    private final JsonUtil jsonUtil;
    private final KafkaProducer producer;
    private final PaymentRepository paymentRepository;

    public PaymentService(JsonUtil jsonUtil, KafkaProducer producer, PaymentRepository paymentRepository) {
        this.jsonUtil = jsonUtil;
        this.producer = producer;
        this.paymentRepository = paymentRepository;
    }

    public void doPayment(Event event) {
        try {
            checkCurrentValidation(event);
            createPendingPayment(event);

            Payment payment = paymentRepository.findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                    .orElseThrow(() -> new ValidationException("Payment not found by OrderId and TransactionId."));

            payment.validateAmount();
            event.addHistorySuccess(CURRENT_SOURCE);

        } catch (Exception ex) {
            log.error("Error trying to make payment: ", ex);
            event.addHistoryFail(ex.getMessage(), CURRENT_SOURCE);
        }

        producer.sendEvent(jsonUtil.toJson(event), "ALTERAR");
    }

    private void checkCurrentValidation(Event event) {
        if (paymentRepository.existsByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())) {
            throw new ValidationException("There's another transactionId for this validation.");
        }
    }

    private void createPendingPayment(Event event) {
        Payment payment = Payment.builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .totalAmount(event.calculateAmount())
                .totalItems(event.calculateTotalItems())
                .build();

        paymentRepository.save(payment);

        event.setTotalAmountItems(payment.getTotalAmount());
        event.setTotalItems(payment.getTotalItems());
    }

    public void doRefund(Event event) {
        event.setStatus(ESagaStatus.FAIL);
        event.setSource(CURRENT_SOURCE);
        try {
            changePaymentStatusToRefund(event);
            event.addHistory(event, "Rollback executed for payment!");
        } catch (Exception ex) {
            event.addHistoryFail("- Rollback failed: ".concat(ex.getMessage()), CURRENT_SOURCE);
        }

        producer.sendEvent(jsonUtil.toJson(event), "ALTERAR");
    }

    private void changePaymentStatusToRefund(Event event) {
        Payment payment = findByOrderIdAndTransactionId(event);
        payment.setStatus(EPaymentStatus.REFUND);
        event.setTotalAmountItems(payment.getTotalAmount());

        paymentRepository.save(payment);
    }

    private Payment findByOrderIdAndTransactionId(Event event) {
        return paymentRepository.findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                .orElseThrow(() -> new ValidationException("Payment not found by OrderId and TransactionId."));
    }
}
