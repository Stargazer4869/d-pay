package com.dpay.payment.domain;

import java.time.Instant;
import java.util.UUID;

public record RefundRecord(
        UUID id,
        UUID paymentId,
        String merchantId,
        long amountMinor,
        String currency,
        RefundStatus status,
        int processingAttempts,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
