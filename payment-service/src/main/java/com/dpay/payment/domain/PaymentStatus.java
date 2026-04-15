package com.dpay.payment.domain;

public enum PaymentStatus {
    PENDING,
    PROCESSING,
    SUCCEEDED,
    FAILED,
    CANCELED,
    PARTIALLY_REFUNDED,
    REFUNDED
}
