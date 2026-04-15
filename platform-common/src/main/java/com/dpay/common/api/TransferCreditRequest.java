package com.dpay.common.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record TransferCreditRequest(
        @NotBlank String merchantId,
        @NotBlank String transferId,
        @Min(1) long amountMinor,
        @NotBlank String currency
) {
}
