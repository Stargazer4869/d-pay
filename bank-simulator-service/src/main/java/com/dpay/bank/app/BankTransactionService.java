package com.dpay.bank.app;

import com.dpay.bank.domain.BankOperationType;
import com.dpay.bank.domain.BankTransaction;
import com.dpay.bank.persistence.BankTransactionRepository;
import com.dpay.common.api.BankOperationRequest;
import com.dpay.common.api.BankOperationResponse;
import java.time.Instant;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BankTransactionService {

    private final BankTransactionRepository repository;

    public BankTransactionService(BankTransactionRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public BankOperationResponse withdraw(BankOperationRequest request) {
        return apply(BankOperationType.WITHDRAWAL, request);
    }

    @Transactional
    public BankOperationResponse deposit(BankOperationRequest request) {
        return apply(BankOperationType.DEPOSIT, request);
    }

    public BankTransaction find(String operationKey) {
        return repository.findByOperationKey(operationKey);
    }

    private BankOperationResponse apply(BankOperationType operationType, BankOperationRequest request) {
        BankTransaction transaction = repository.findByOperationKey(request.operationKey());
        if (transaction == null) {
            transaction = new BankTransaction(
                    request.operationKey(),
                    operationType,
                    request.accountRef(),
                    request.amountMinor(),
                    request.currency(),
                    request.reference(),
                    "PENDING",
                    0,
                    failureBudget(operationType, request.accountRef()),
                    false,
                    Instant.now(),
                    Instant.now());
            repository.insert(transaction);
        }

        if (transaction.terminal()) {
            return mapResponse(transaction);
        }

        int attemptCount = transaction.attemptCount() + 1;
        if (isPermanentFailure(operationType, transaction.accountRef())) {
            repository.updateStatus(transaction.operationKey(), "PERMANENT_FAILURE", attemptCount, true);
            return new BankOperationResponse(transaction.operationKey(), "PERMANENT_FAILURE", false, false,
                    "permanent simulated bank failure");
        }

        if (attemptCount <= transaction.failureBudget()) {
            repository.updateStatus(transaction.operationKey(), "RETRYABLE_FAILURE", attemptCount, false);
            return new BankOperationResponse(transaction.operationKey(), "RETRYABLE_FAILURE", false, true,
                    "transient simulated bank failure");
        }

        repository.updateStatus(transaction.operationKey(), "SUCCEEDED", attemptCount, true);
        return new BankOperationResponse(transaction.operationKey(), "SUCCEEDED", true, false, "bank operation succeeded");
    }

    private BankOperationResponse mapResponse(BankTransaction transaction) {
        return switch (transaction.status()) {
            case "SUCCEEDED" -> new BankOperationResponse(
                    transaction.operationKey(), transaction.status(), true, false, "bank operation succeeded");
            case "PERMANENT_FAILURE" -> new BankOperationResponse(
                    transaction.operationKey(), transaction.status(), false, false, "permanent simulated bank failure");
            case "RETRYABLE_FAILURE" -> new BankOperationResponse(
                    transaction.operationKey(), transaction.status(), false, true, "transient simulated bank failure");
            default -> new BankOperationResponse(transaction.operationKey(), transaction.status(), false, false, "pending");
        };
    }

    private boolean isPermanentFailure(BankOperationType operationType, String accountRef) {
        return accountRef.startsWith("fail-" + prefix(operationType));
    }

    private int failureBudget(BankOperationType operationType, String accountRef) {
        String expectedPrefix = "transient-" + prefix(operationType);
        if (!accountRef.startsWith(expectedPrefix)) {
            return 0;
        }
        int lastDash = accountRef.lastIndexOf('-');
        if (lastDash > expectedPrefix.length()) {
            return Integer.parseInt(accountRef.substring(lastDash + 1));
        }
        return 1;
    }

    private String prefix(BankOperationType operationType) {
        return operationType == BankOperationType.WITHDRAWAL ? "withdraw" : "deposit";
    }
}
