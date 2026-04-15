package com.dpay.common.outbox;

import java.time.Instant;
import java.util.UUID;

public record OutboxMessage(
        UUID id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        Instant createdAt
) {
}
