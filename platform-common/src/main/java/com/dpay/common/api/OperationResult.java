package com.dpay.common.api;

public record OperationResult(
        boolean success,
        boolean retryable,
        String message
) {

    public static OperationResult success(String message) {
        return new OperationResult(true, false, message);
    }

    public static OperationResult failure(String message) {
        return new OperationResult(false, false, message);
    }

    public static OperationResult retryableFailure(String message) {
        return new OperationResult(false, true, message);
    }
}
