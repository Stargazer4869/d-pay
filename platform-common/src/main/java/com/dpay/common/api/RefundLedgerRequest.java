package com.dpay.common.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record RefundLedgerRequest(
        @NotBlank String merchantId,
        @NotBlank String refundId,
        @NotBlank String paymentId,
        @Min(1) long amountMinor,
        @NotBlank String currency
) {
}
