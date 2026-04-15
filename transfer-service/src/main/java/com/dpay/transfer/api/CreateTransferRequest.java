package com.dpay.transfer.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateTransferRequest(
        @NotBlank String sourceMerchantId,
        @NotBlank String destinationMerchantId,
        @NotBlank String merchantReference,
        @Min(1) long amountMinor,
        @NotBlank String currency
) {
}
