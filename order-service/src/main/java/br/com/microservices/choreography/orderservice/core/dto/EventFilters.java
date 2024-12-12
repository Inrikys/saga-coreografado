package br.com.microservices.choreography.orderservice.core.dto;

import br.com.microservices.choreography.orderservice.config.exception.ValidationException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static org.springframework.util.ObjectUtils.isEmpty;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventFilters {

    private String orderId;
    private String transactionId;


    public void isValid() {
        if (isEmpty(orderId) && isEmpty(transactionId)) {
            throw new ValidationException("OrderId or TransactionId must be informed");
        }
    }
}
