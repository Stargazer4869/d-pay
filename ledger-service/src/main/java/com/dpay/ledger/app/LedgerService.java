package com.dpay.ledger.app;

import com.dpay.common.api.LedgerBalanceResponse;
import com.dpay.common.api.OperationResult;
import com.dpay.common.api.PaymentLedgerCreditRequest;
import com.dpay.common.api.RefundLedgerRequest;
import com.dpay.common.api.TransferCreditRequest;
import com.dpay.common.api.TransferReserveRequest;
import com.dpay.common.json.JsonUtils;
import com.dpay.ledger.domain.LedgerAccount;
import com.dpay.ledger.domain.LedgerOperationType;
import com.dpay.ledger.persistence.LedgerRepository;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.dpay.sharding.core.ShardRoutingManager;
import org.springframework.stereotype.Service;

@Service
public class LedgerService {

    private final ShardRoutingManager shardRoutingManager;
    private final LedgerRepository ledgerRepository;
    private final ConcurrentHashMap<String, AtomicInteger> transferCreditAttempts = new ConcurrentHashMap<>();

    public LedgerService(ShardRoutingManager shardRoutingManager, LedgerRepository ledgerRepository) {
        this.shardRoutingManager = shardRoutingManager;
        this.ledgerRepository = ledgerRepository;
    }

    public OperationResult creditPayment(PaymentLedgerCreditRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            ledgerRepository.ensureAccount(context, request.merchantId(), request.currency());
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.PAYMENT_CREDIT, request.paymentId())) {
                return OperationResult.success("payment credit already applied");
            }
            ledgerRepository.updateBalance(context, request.merchantId(), request.currency(), request.amountMinor(), 0, 0);
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.PAYMENT_CREDIT.name(),
                    request.paymentId(),
                    "AVAILABLE",
                    request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.PAYMENT_CREDIT, request.paymentId());
            return OperationResult.success("payment credited");
        });
    }

    public OperationResult reserveTransfer(TransferReserveRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            ledgerRepository.ensureAccount(context, request.merchantId(), request.currency());
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_RESERVE, request.transferId())) {
                return OperationResult.success("transfer reserve already applied");
            }
            int updated = ledgerRepository.reserveOutgoing(context, request.merchantId(), request.currency(), request.amountMinor());
            if (updated == 0) {
                return OperationResult.failure("insufficient available balance");
            }
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.TRANSFER_RESERVE.name(),
                    request.transferId(),
                    "AVAILABLE",
                    -request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.TRANSFER_RESERVE.name(),
                    request.transferId() + "#reserved",
                    "RESERVED_OUTGOING",
                    request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_RESERVE, request.transferId());
            return OperationResult.success("transfer reserved");
        });
    }

    public OperationResult finalizeTransfer(TransferReserveRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_FINALIZE, request.transferId())) {
                return OperationResult.success("transfer finalization already applied");
            }
            int updated = ledgerRepository.finalizeOutgoing(context, request.merchantId(), request.currency(), request.amountMinor());
            if (updated == 0) {
                return OperationResult.failure("reserved outgoing balance not available");
            }
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.TRANSFER_FINALIZE.name(),
                    request.transferId(),
                    "RESERVED_OUTGOING",
                    -request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_FINALIZE, request.transferId());
            return OperationResult.success("transfer finalized");
        });
    }

    public OperationResult releaseTransfer(TransferReserveRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_RELEASE, request.transferId())) {
                return OperationResult.success("transfer release already applied");
            }
            int updated = ledgerRepository.releaseOutgoing(context, request.merchantId(), request.currency(), request.amountMinor());
            if (updated == 0) {
                return OperationResult.failure("reserved outgoing balance not available");
            }
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.TRANSFER_RELEASE.name(),
                    request.transferId(),
                    "AVAILABLE",
                    request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.TRANSFER_RELEASE.name(),
                    request.transferId() + "#reserved",
                    "RESERVED_OUTGOING",
                    -request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_RELEASE, request.transferId());
            return OperationResult.success("transfer released");
        });
    }

    public OperationResult creditTransfer(TransferCreditRequest request) {
        if (request.merchantId().startsWith("fail-credit")) {
            return OperationResult.failure("simulated destination credit failure");
        }
        if (request.merchantId().startsWith("retry-credit")) {
            int failureBudget = extractFailureBudget(request.merchantId());
            int attempt = transferCreditAttempts
                    .computeIfAbsent(request.transferId(), ignored -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt <= failureBudget) {
                return OperationResult.retryableFailure("simulated retryable destination credit failure");
            }
        }
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            ledgerRepository.ensureAccount(context, request.merchantId(), request.currency());
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_CREDIT, request.transferId())) {
                return OperationResult.success("transfer credit already applied");
            }
            ledgerRepository.updateBalance(context, request.merchantId(), request.currency(), request.amountMinor(), 0, 0);
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.TRANSFER_CREDIT.name(),
                    request.transferId(),
                    "AVAILABLE",
                    request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.TRANSFER_CREDIT, request.transferId());
            return OperationResult.success("incoming transfer credited");
        });
    }

    public OperationResult reserveRefund(RefundLedgerRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            ledgerRepository.ensureAccount(context, request.merchantId(), request.currency());
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.REFUND_RESERVE, request.refundId())) {
                return OperationResult.success("refund reserve already applied");
            }
            int updated = ledgerRepository.reserveRefund(context, request.merchantId(), request.currency(), request.amountMinor());
            if (updated == 0) {
                return OperationResult.failure("insufficient available balance for refund");
            }
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.REFUND_RESERVE.name(),
                    request.refundId(),
                    "AVAILABLE",
                    -request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.REFUND_RESERVE.name(),
                    request.refundId() + "#reserved",
                    "RESERVED_REFUND",
                    request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.REFUND_RESERVE, request.refundId());
            return OperationResult.success("refund reserved");
        });
    }

    public OperationResult finalizeRefund(RefundLedgerRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.REFUND_FINALIZE, request.refundId())) {
                return OperationResult.success("refund finalization already applied");
            }
            int updated = ledgerRepository.finalizeRefund(context, request.merchantId(), request.currency(), request.amountMinor());
            if (updated == 0) {
                return OperationResult.failure("reserved refund balance not available");
            }
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.REFUND_FINALIZE.name(),
                    request.refundId(),
                    "RESERVED_REFUND",
                    -request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.REFUND_FINALIZE, request.refundId());
            return OperationResult.success("refund finalized");
        });
    }

    public OperationResult releaseRefund(RefundLedgerRequest request) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            if (ledgerRepository.isOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.REFUND_RELEASE, request.refundId())) {
                return OperationResult.success("refund release already applied");
            }
            int updated = ledgerRepository.releaseRefund(context, request.merchantId(), request.currency(), request.amountMinor());
            if (updated == 0) {
                return OperationResult.failure("reserved refund balance not available");
            }
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.REFUND_RELEASE.name(),
                    request.refundId(),
                    "AVAILABLE",
                    request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.insertJournalEntry(
                    context,
                    request.merchantId(),
                    request.currency(),
                    LedgerOperationType.REFUND_RELEASE.name(),
                    request.refundId() + "#reserved",
                    "RESERVED_REFUND",
                    -request.amountMinor(),
                    JsonUtils.toJson(request));
            ledgerRepository.markOperationApplied(
                    context, request.merchantId(), request.currency(), LedgerOperationType.REFUND_RELEASE, request.refundId());
            return OperationResult.success("refund released");
        });
    }

    public LedgerBalanceResponse getBalance(String merchantId, String currency) {
        LedgerAccount account = shardRoutingManager.inShardTransactionForMerchant(merchantId,
                context -> ledgerRepository.findAccount(context, merchantId, currency));
        return new LedgerBalanceResponse(
                account.merchantId(),
                account.currency(),
                account.availableBalance(),
                account.reservedOutgoing(),
                account.reservedRefund());
    }

    private int extractFailureBudget(String merchantId) {
        int lastDash = merchantId.lastIndexOf('-');
        if (lastDash > 0 && lastDash < merchantId.length() - 1) {
            return Integer.parseInt(merchantId.substring(lastDash + 1));
        }
        return 1;
    }
}
