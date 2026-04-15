package com.dpay.notification.domain;

import java.time.Instant;
import java.util.UUID;

public record WebhookDeliveryRecord(
        UUID eventId,
        String merchantId,
        String targetUrl,
        String secret,
        String payload,
        String status,
        int attemptCount,
        Integer lastResponseCode,
        String lastError,
        Instant nextAttemptAt
) {
}
