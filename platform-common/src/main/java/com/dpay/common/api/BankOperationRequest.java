package com.dpay.common.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record BankOperationRequest(
        @NotBlank String operationKey,
        @NotBlank String accountRef,
        @Min(1) long amountMinor,
        @NotBlank String currency,
        @NotBlank String reference
) {
}
