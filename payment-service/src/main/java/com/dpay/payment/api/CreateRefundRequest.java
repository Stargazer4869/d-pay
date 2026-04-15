package com.dpay.payment.api;

import jakarta.validation.constraints.Min;

public record CreateRefundRequest(
        @Min(1) long amountMinor
) {
}
