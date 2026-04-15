package com.dpay.payment.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Instant;

public record CreatePaymentRequest(
        @NotBlank String merchantId,
        @NotBlank String merchantReference,
        @Min(1) long amountMinor,
        @NotBlank String currency,
        @NotBlank String payerRef,
        @NotBlank String payeeRef,
        Instant expiresAt
) {
}
