package com.dpay.payment.domain;

import java.time.Instant;
import java.util.UUID;

public record PaymentRecord(
        UUID id,
        String merchantId,
        String merchantReference,
        long amountMinor,
        String currency,
        String payerRef,
        String payeeRef,
        PaymentStatus status,
        int processingAttempts,
        String lastError,
        Instant expiresAt,
        Instant createdAt,
        Instant updatedAt
) {
}
