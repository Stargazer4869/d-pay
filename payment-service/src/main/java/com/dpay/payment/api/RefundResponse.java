package com.dpay.payment.api;

import java.time.Instant;
import java.util.UUID;

public record RefundResponse(
        UUID id,
        UUID paymentId,
        String merchantId,
        long amountMinor,
        String currency,
        String status,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
