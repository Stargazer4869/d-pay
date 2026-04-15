package com.dpay.notification.api;

import jakarta.validation.constraints.NotBlank;

public record WebhookSubscriptionRequest(
        @NotBlank String targetUrl,
        @NotBlank String secret,
        boolean active
) {
}
