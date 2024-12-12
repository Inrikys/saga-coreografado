package br.com.microservices.choreography.paymentservice.core.model;

import br.com.microservices.choreography.paymentservice.config.exception.ValidationException;
import br.com.microservices.choreography.paymentservice.core.enums.EPaymentStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "payment")
public class Payment {

    private static final Double MIN_AMOUNT_VALUE = 0.1;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String transactionId;

    @Column(nullable = false)
    private int totalItems;

    @Column(nullable = false)
    private double totalAmount;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private EPaymentStatus status;

    @Column(nullable = false, updatable = false)
    @CreationTimestamp
    private LocalDateTime createdAt;

    @Column(nullable = false)
    @UpdateTimestamp
    private LocalDateTime updatedAt;


    public void validateAmount() {
        if (totalAmount < MIN_AMOUNT_VALUE) {
            throw new ValidationException("The minimum amount available is ".concat(MIN_AMOUNT_VALUE.toString()));
        }
    }
}
