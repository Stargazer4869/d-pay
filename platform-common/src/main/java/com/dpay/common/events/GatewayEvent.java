package com.dpay.common.events;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record GatewayEvent(
        UUID eventId,
        GatewayEventType type,
        String aggregateType,
        String aggregateId,
        String merchantId,
        Long amountMinor,
        String currency,
        Instant occurredAt,
        Map<String, String> metadata
) {
}
