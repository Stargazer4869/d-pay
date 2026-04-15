package com.dpay.transfer.api;

import java.time.Instant;
import java.util.UUID;

public record TransferResponse(
        UUID id,
        String sourceMerchantId,
        String destinationMerchantId,
        String merchantReference,
        long amountMinor,
        String currency,
        String status,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
