package com.dpay.payment.app;

import com.dpay.common.api.BankOperationRequest;
import com.dpay.common.api.BankOperationResponse;
import com.dpay.common.api.OperationResult;
import com.dpay.common.api.PaymentLedgerCreditRequest;
import com.dpay.common.api.RefundLedgerRequest;
import com.dpay.common.events.GatewayEvent;
import com.dpay.common.events.GatewayEventType;
import com.dpay.common.json.JsonUtils;
import com.dpay.payment.domain.PaymentRecord;
import com.dpay.payment.domain.PaymentStatus;
import com.dpay.payment.domain.RefundRecord;
import com.dpay.payment.domain.RefundStatus;
import com.dpay.payment.integration.BankHttpClient;
import com.dpay.payment.integration.LedgerHttpClient;
import com.dpay.payment.persistence.PaymentRepository;
import com.dpay.sharding.core.ShardJdbcContext;
import com.dpay.sharding.core.ShardRoutingManager;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class PaymentProcessingService {

    private final ShardRoutingManager shardRoutingManager;
    private final PaymentRepository paymentRepository;
    private final BankHttpClient bankHttpClient;
    private final LedgerHttpClient ledgerHttpClient;
    private final int maxAttempts;

    public PaymentProcessingService(
            ShardRoutingManager shardRoutingManager,
            PaymentRepository paymentRepository,
            BankHttpClient bankHttpClient,
            LedgerHttpClient ledgerHttpClient,
            @Value("${app.processing.max-attempts:3}") int maxAttempts) {
        this.shardRoutingManager = shardRoutingManager;
        this.paymentRepository = paymentRepository;
        this.bankHttpClient = bankHttpClient;
        this.ledgerHttpClient = ledgerHttpClient;
        this.maxAttempts = maxAttempts;
    }

    public void handleEvent(GatewayEvent event) {
        if (event.type() == GatewayEventType.PAYMENT_CONFIRM_REQUESTED) {
            processConfirmation(event);
        } else if (event.type() == GatewayEventType.REFUND_REQUESTED) {
            processRefund(event);
        }
    }

    private void processConfirmation(GatewayEvent event) {
        String merchantId = event.merchantId();
        UUID paymentId = UUID.fromString(event.aggregateId());
        PaymentRecord payment = shardRoutingManager.inShardTransactionForMerchant(merchantId, context ->
                paymentRepository.findPaymentForUpdate(context, paymentId));
        if (payment == null || payment.status() != PaymentStatus.PROCESSING) {
            return;
        }

        BankOperationResponse response = bankHttpClient.withdraw(new BankOperationRequest(
                payment.id().toString(),
                payment.payerRef(),
                payment.amountMinor(),
                payment.currency(),
                payment.merchantReference()));

        if (!response.success()) {
            if (response.retryable()) {
                handleRetryablePaymentFailure(payment, response.message());
                throw new RetryableProcessingException(response.message());
            }
            markPaymentFailed(payment, response.message());
            return;
        }

        OperationResult creditResult = ledgerHttpClient.creditPayment(new PaymentLedgerCreditRequest(
                payment.merchantId(),
                payment.id().toString(),
                payment.amountMinor(),
                payment.currency()));
        if (!creditResult.success()) {
            throw new RetryableProcessingException(creditResult.message());
        }

        shardRoutingManager.inShardTransactionForMerchant(merchantId, context -> {
            PaymentRecord locked = paymentRepository.findPaymentForUpdate(context, paymentId);
            if (locked.status() != PaymentStatus.PROCESSING) {
                return null;
            }
            paymentRepository.updatePaymentState(context, paymentId, PaymentStatus.SUCCEEDED, locked.processingAttempts(), null);
            PaymentRecord updated = paymentRepository.findPaymentForUpdate(context, paymentId);
            writePaymentEvent(context, updated, GatewayEventType.PAYMENT_SUCCEEDED, Map.of("bankStatus", response.status()));
            return null;
        });
    }

    private void processRefund(GatewayEvent event) {
        String merchantId = event.merchantId();
        UUID refundId = UUID.fromString(event.aggregateId());
        RefundRecord refund = shardRoutingManager.inShardTransactionForMerchant(merchantId, context ->
                paymentRepository.findRefundForUpdate(context, refundId));
        if (refund == null || refund.status() != RefundStatus.PROCESSING) {
            return;
        }

        PaymentRecord payment = shardRoutingManager.inShardTransactionForMerchant(merchantId, context ->
                paymentRepository.findPaymentForUpdate(context, refund.paymentId()));

        RefundLedgerRequest ledgerRequest = new RefundLedgerRequest(
                refund.merchantId(),
                refund.id().toString(),
                refund.paymentId().toString(),
                refund.amountMinor(),
                refund.currency());
        OperationResult reserveResult = ledgerHttpClient.reserveRefund(ledgerRequest);
        if (!reserveResult.success()) {
            markRefundFailed(refund, reserveResult.message());
            return;
        }

        BankOperationResponse bankResponse = bankHttpClient.deposit(new BankOperationRequest(
                refund.id().toString(),
                payment.payerRef(),
                refund.amountMinor(),
                refund.currency(),
                refund.paymentId().toString()));
        if (!bankResponse.success()) {
            if (bankResponse.retryable()) {
                handleRetryableRefundFailure(refund, bankResponse.message());
                throw new RetryableProcessingException(bankResponse.message());
            }
            ledgerHttpClient.releaseRefund(ledgerRequest);
            markRefundFailed(refund, bankResponse.message());
            return;
        }

        OperationResult finalizeResult = ledgerHttpClient.finalizeRefund(ledgerRequest);
        if (!finalizeResult.success()) {
            throw new RetryableProcessingException(finalizeResult.message());
        }

        shardRoutingManager.inShardTransactionForMerchant(merchantId, context -> {
            RefundRecord lockedRefund = paymentRepository.findRefundForUpdate(context, refund.id());
            if (lockedRefund.status() != RefundStatus.PROCESSING) {
                return null;
            }
            paymentRepository.updateRefundState(context, refund.id(), RefundStatus.SUCCEEDED, lockedRefund.processingAttempts(), null);
            long totalRefunded = paymentRepository.sumSuccessfulRefundAmount(context, refund.paymentId());
            PaymentStatus nextStatus = totalRefunded >= payment.amountMinor() ? PaymentStatus.REFUNDED : PaymentStatus.PARTIALLY_REFUNDED;
            paymentRepository.updatePaymentState(context, payment.id(), nextStatus, payment.processingAttempts(), payment.lastError());
            RefundRecord updatedRefund = paymentRepository.findRefundForUpdate(context, refund.id());
            PaymentRecord updatedPayment = paymentRepository.findPaymentForUpdate(context, payment.id());
            writeRefundEvent(context, updatedRefund, GatewayEventType.REFUND_SUCCEEDED, Map.of("paymentStatus", nextStatus.name()));
            writePaymentEvent(context, updatedPayment, GatewayEventType.REFUND_SUCCEEDED, Map.of("refundId", refund.id().toString()));
            return null;
        });
    }

    private void handleRetryablePaymentFailure(PaymentRecord payment, String errorMessage) {
        shardRoutingManager.inShardTransactionForMerchant(payment.merchantId(), context -> {
            PaymentRecord locked = paymentRepository.findPaymentForUpdate(context, payment.id());
            int nextAttempt = locked.processingAttempts() + 1;
            if (nextAttempt >= maxAttempts) {
                paymentRepository.updatePaymentState(context, locked.id(), PaymentStatus.FAILED, nextAttempt, errorMessage);
                PaymentRecord updated = paymentRepository.findPaymentForUpdate(context, locked.id());
                writePaymentEvent(context, updated, GatewayEventType.PAYMENT_FAILED, Map.of("reason", errorMessage));
            } else {
                paymentRepository.updatePaymentState(context, locked.id(), PaymentStatus.PROCESSING, nextAttempt, errorMessage);
            }
            return null;
        });
    }

    private void markPaymentFailed(PaymentRecord payment, String errorMessage) {
        shardRoutingManager.inShardTransactionForMerchant(payment.merchantId(), context -> {
            PaymentRecord locked = paymentRepository.findPaymentForUpdate(context, payment.id());
            paymentRepository.updatePaymentState(context, locked.id(), PaymentStatus.FAILED, locked.processingAttempts(), errorMessage);
            PaymentRecord updated = paymentRepository.findPaymentForUpdate(context, locked.id());
            writePaymentEvent(context, updated, GatewayEventType.PAYMENT_FAILED, Map.of("reason", errorMessage));
            return null;
        });
    }

    private void handleRetryableRefundFailure(RefundRecord refund, String errorMessage) {
        shardRoutingManager.inShardTransactionForMerchant(refund.merchantId(), context -> {
            RefundRecord locked = paymentRepository.findRefundForUpdate(context, refund.id());
            int nextAttempt = locked.processingAttempts() + 1;
            if (nextAttempt >= maxAttempts) {
                ledgerHttpClient.releaseRefund(new RefundLedgerRequest(
                        locked.merchantId(),
                        locked.id().toString(),
                        locked.paymentId().toString(),
                        locked.amountMinor(),
                        locked.currency()));
                paymentRepository.updateRefundState(context, locked.id(), RefundStatus.FAILED, nextAttempt, errorMessage);
                RefundRecord updated = paymentRepository.findRefundForUpdate(context, locked.id());
                writeRefundEvent(context, updated, GatewayEventType.REFUND_FAILED, Map.of("reason", errorMessage));
            } else {
                paymentRepository.updateRefundState(context, locked.id(), RefundStatus.PROCESSING, nextAttempt, errorMessage);
            }
            return null;
        });
    }

    private void markRefundFailed(RefundRecord refund, String errorMessage) {
        shardRoutingManager.inShardTransactionForMerchant(refund.merchantId(), context -> {
            RefundRecord locked = paymentRepository.findRefundForUpdate(context, refund.id());
            paymentRepository.updateRefundState(context, locked.id(), RefundStatus.FAILED, locked.processingAttempts(), errorMessage);
            RefundRecord updated = paymentRepository.findRefundForUpdate(context, locked.id());
            writeRefundEvent(context, updated, GatewayEventType.REFUND_FAILED, Map.of("reason", errorMessage));
            return null;
        });
    }

    private void writePaymentEvent(ShardJdbcContext context, PaymentRecord payment, GatewayEventType type, Map<String, String> details) {
        paymentRepository.appendPaymentEvent(context, payment.id(), payment.merchantId(), type.name(), payment.status(), JsonUtils.toJson(details));
        GatewayEvent event = new GatewayEvent(
                UUID.randomUUID(),
                type,
                "PAYMENT",
                payment.id().toString(),
                payment.merchantId(),
                payment.amountMinor(),
                payment.currency(),
                Instant.now(),
                details);
        paymentRepository.appendOutboxEvent(
                context, event.eventId(), event.aggregateType(), payment.id(), payment.merchantId(), type.name(), JsonUtils.toJson(event));
    }

    private void writeRefundEvent(ShardJdbcContext context, RefundRecord refund, GatewayEventType type, Map<String, String> details) {
        paymentRepository.appendRefundEvent(
                context, refund.id(), refund.paymentId(), refund.merchantId(), type.name(), refund.status(), JsonUtils.toJson(details));
        GatewayEvent event = new GatewayEvent(
                UUID.randomUUID(),
                type,
                "REFUND",
                refund.id().toString(),
                refund.merchantId(),
                refund.amountMinor(),
                refund.currency(),
                Instant.now(),
                details);
        paymentRepository.appendOutboxEvent(
                context, event.eventId(), event.aggregateType(), refund.id(), refund.merchantId(), type.name(), JsonUtils.toJson(event));
    }

    public static class RetryableProcessingException extends RuntimeException {
        public RetryableProcessingException(String message) {
            super(message);
        }
    }
}
