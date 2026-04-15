package com.dpay.payment.api;

import java.time.Instant;

public record HistoryItemResponse(
        String resourceType,
        String resourceId,
        String eventType,
        String status,
        String details,
        Instant createdAt
) {
}
