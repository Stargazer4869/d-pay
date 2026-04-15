package com.dpay.payment.api;

import java.time.Instant;
import java.util.UUID;

public record PaymentResponse(
        UUID id,
        String merchantId,
        String merchantReference,
        long amountMinor,
        String currency,
        String payerRef,
        String payeeRef,
        String status,
        long refundedAmountMinor,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
