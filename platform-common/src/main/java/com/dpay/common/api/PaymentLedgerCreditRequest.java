package com.dpay.common.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record PaymentLedgerCreditRequest(
        @NotBlank String merchantId,
        @NotBlank String paymentId,
        @Min(1) long amountMinor,
        @NotBlank String currency
) {
}
