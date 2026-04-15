package com.dpay.transfer.domain;

import java.time.Instant;
import java.util.UUID;

public record TransferRecord(
        UUID id,
        String sourceMerchantId,
        String destinationMerchantId,
        String merchantReference,
        long amountMinor,
        String currency,
        TransferStatus status,
        String sourceShardId,
        String destinationShardId,
        int attemptCount,
        Instant nextRetryAt,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
