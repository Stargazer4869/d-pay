package com.dpay.common.api;

public record BankOperationResponse(
        String operationKey,
        String status,
        boolean success,
        boolean retryable,
        String message
) {
}
