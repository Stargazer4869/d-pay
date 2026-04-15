package com.dpay.bank.domain;

import java.time.Instant;

public record BankTransaction(
        String operationKey,
        BankOperationType operationType,
        String accountRef,
        long amountMinor,
        String currency,
        String reference,
        String status,
        int attemptCount,
        int failureBudget,
        boolean terminal,
        Instant createdAt,
        Instant updatedAt
) {
}
