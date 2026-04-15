package com.dpay.transfer.api;

import java.time.Instant;

public record TransferEventResponse(
        String eventType,
        String status,
        String details,
        Instant createdAt
) {
}
