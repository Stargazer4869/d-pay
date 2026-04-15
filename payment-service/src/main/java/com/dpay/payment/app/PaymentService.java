package com.dpay.payment.app;

import com.dpay.common.api.ApiException;
import com.dpay.common.events.GatewayEvent;
import com.dpay.common.events.GatewayEventType;
import com.dpay.common.json.JsonUtils;
import com.dpay.payment.api.CreatePaymentRequest;
import com.dpay.payment.api.CreateRefundRequest;
import com.dpay.payment.api.HistoryItemResponse;
import com.dpay.payment.api.PaymentResponse;
import com.dpay.payment.api.RefundResponse;
import com.dpay.payment.domain.PaymentRecord;
import com.dpay.payment.domain.PaymentStatus;
import com.dpay.payment.domain.RefundRecord;
import com.dpay.payment.domain.RefundStatus;
import com.dpay.payment.persistence.PaymentRepository;
import com.dpay.sharding.core.ShardJdbcContext;
import com.dpay.sharding.core.ShardRoutingManager;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    private static final String CREATE_PAYMENT_OPERATION = "CREATE_PAYMENT";
    private static final String CONFIRM_PAYMENT_OPERATION = "CONFIRM_PAYMENT";
    private static final String CANCEL_PAYMENT_OPERATION = "CANCEL_PAYMENT";
    private static final String CREATE_REFUND_OPERATION = "CREATE_REFUND";

    private final ShardRoutingManager shardRoutingManager;
    private final PaymentRepository paymentRepository;

    public PaymentService(ShardRoutingManager shardRoutingManager, PaymentRepository paymentRepository) {
        this.shardRoutingManager = shardRoutingManager;
        this.paymentRepository = paymentRepository;
    }

    public PaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {
        return shardRoutingManager.inShardTransactionForMerchant(request.merchantId(), context -> {
            String cached = paymentRepository.findIdempotentResponse(
                    context, request.merchantId(), CREATE_PAYMENT_OPERATION, idempotencyKey);
            if (cached != null) {
                return JsonUtils.fromJson(cached, PaymentResponse.class);
            }

            Instant now = Instant.now();
            PaymentRecord payment = new PaymentRecord(
                    UUID.randomUUID(),
                    request.merchantId(),
                    request.merchantReference(),
                    request.amountMinor(),
                    request.currency(),
                    request.payerRef(),
                    request.payeeRef(),
                    PaymentStatus.PENDING,
                    0,
                    null,
                    request.expiresAt(),
                    now,
                    now);
            paymentRepository.insertPayment(context, payment);
            writePaymentEvent(context, payment, GatewayEventType.PAYMENT_CREATED, Map.of("merchantReference", request.merchantReference()));
            PaymentResponse response = toPaymentResponse(context, payment);
            paymentRepository.saveIdempotentResponse(
                    context, request.merchantId(), CREATE_PAYMENT_OPERATION, idempotencyKey, JsonUtils.toJson(response));
            return response;
        });
    }

    public PaymentResponse confirmPayment(String merchantId, UUID paymentId, String idempotencyKey) {
        return shardRoutingManager.inShardTransactionForMerchant(merchantId, context -> {
            String cached = paymentRepository.findIdempotentResponse(
                    context, merchantId, CONFIRM_PAYMENT_OPERATION, idempotencyKey);
            if (cached != null) {
                return JsonUtils.fromJson(cached, PaymentResponse.class);
            }

            PaymentRecord payment = requiredPayment(context, paymentId);
            if (payment.status() == PaymentStatus.CANCELED) {
                throw new ApiException(HttpStatus.CONFLICT, "invalid_state", "Canceled payment cannot be confirmed");
            }
            if (payment.status() != PaymentStatus.PENDING) {
                throw new ApiException(HttpStatus.CONFLICT, "invalid_state", "Only pending payments can be confirmed");
            }
            paymentRepository.updatePaymentState(context, paymentId, PaymentStatus.PROCESSING, 0, null);
            PaymentRecord updated = requiredPayment(context, paymentId);
            writePaymentEvent(context, updated, GatewayEventType.PAYMENT_CONFIRM_REQUESTED, Map.of());
            PaymentResponse response = toPaymentResponse(context, updated);
            paymentRepository.saveIdempotentResponse(
                    context, merchantId, CONFIRM_PAYMENT_OPERATION, idempotencyKey, JsonUtils.toJson(response));
            return response;
        });
    }

    public PaymentResponse cancelPayment(String merchantId, UUID paymentId, String idempotencyKey) {
        return shardRoutingManager.inShardTransactionForMerchant(merchantId, context -> {
            String cached = paymentRepository.findIdempotentResponse(
                    context, merchantId, CANCEL_PAYMENT_OPERATION, idempotencyKey);
            if (cached != null) {
                return JsonUtils.fromJson(cached, PaymentResponse.class);
            }

            PaymentRecord payment = requiredPayment(context, paymentId);
            if (payment.status() != PaymentStatus.PENDING) {
                throw new ApiException(HttpStatus.CONFLICT, "invalid_state", "Only pending payments can be canceled");
            }
            paymentRepository.updatePaymentState(context, paymentId, PaymentStatus.CANCELED, 0, null);
            PaymentRecord updated = requiredPayment(context, paymentId);
            writePaymentEvent(context, updated, GatewayEventType.PAYMENT_CANCELED, Map.of());
            PaymentResponse response = toPaymentResponse(context, updated);
            paymentRepository.saveIdempotentResponse(
                    context, merchantId, CANCEL_PAYMENT_OPERATION, idempotencyKey, JsonUtils.toJson(response));
            return response;
        });
    }

    public RefundResponse createRefund(String merchantId, UUID paymentId, CreateRefundRequest request, String idempotencyKey) {
        return shardRoutingManager.inShardTransactionForMerchant(merchantId, context -> {
            String cached = paymentRepository.findIdempotentResponse(
                    context, merchantId, CREATE_REFUND_OPERATION, idempotencyKey);
            if (cached != null) {
                return JsonUtils.fromJson(cached, RefundResponse.class);
            }

            PaymentRecord payment = requiredPayment(context, paymentId);
            if (!(payment.status() == PaymentStatus.SUCCEEDED
                    || payment.status() == PaymentStatus.PARTIALLY_REFUNDED
                    || payment.status() == PaymentStatus.REFUNDED)) {
                throw new ApiException(HttpStatus.CONFLICT, "invalid_state", "Refunds require a successful payment");
            }
            long blockedAmount = paymentRepository.sumBlockedRefundAmount(context, paymentId);
            if (blockedAmount + request.amountMinor() > payment.amountMinor()) {
                throw new ApiException(HttpStatus.CONFLICT, "refund_limit_exceeded", "Refund amount exceeds captured amount");
            }
            Instant now = Instant.now();
            RefundRecord refund = new RefundRecord(
                    UUID.randomUUID(),
                    paymentId,
                    merchantId,
                    request.amountMinor(),
                    payment.currency(),
                    RefundStatus.PROCESSING,
                    0,
                    null,
                    now,
                    now);
            paymentRepository.insertRefund(context, refund);
            writeRefundEvent(context, refund, GatewayEventType.REFUND_REQUESTED, Map.of("paymentId", paymentId.toString()));
            RefundResponse response = toRefundResponse(refund);
            paymentRepository.saveIdempotentResponse(
                    context, merchantId, CREATE_REFUND_OPERATION, idempotencyKey, JsonUtils.toJson(response));
            return response;
        });
    }

    public PaymentResponse getPayment(String merchantId, UUID paymentId) {
        return shardRoutingManager.inShardTransactionForMerchant(merchantId, context -> toPaymentResponse(context, requiredPayment(context, paymentId)));
    }

    public List<PaymentResponse> listPayments(String merchantId) {
        return shardRoutingManager.inShardTransactionForMerchant(merchantId, context ->
                paymentRepository.findPaymentsByMerchant(context, merchantId)
                        .stream()
                        .map(payment -> toPaymentResponse(context, payment))
                        .toList());
    }

    public List<HistoryItemResponse> getHistory(String merchantId, UUID paymentId) {
        return shardRoutingManager.inShardTransactionForMerchant(merchantId, context ->
                paymentRepository.findHistory(context, paymentId).stream()
                        .map(row -> new HistoryItemResponse(
                                row.get("resource_type").toString(),
                                row.get("resource_id").toString(),
                                row.get("event_type").toString(),
                                row.get("status").toString(),
                                row.get("details").toString(),
                                ((java.sql.Timestamp) row.get("created_at")).toInstant()))
                        .toList());
    }

    private PaymentRecord requiredPayment(ShardJdbcContext context, UUID paymentId) {
        PaymentRecord payment = paymentRepository.findPaymentForUpdate(context, paymentId);
        if (payment == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "payment_not_found", "Unknown payment");
        }
        return payment;
    }

    private void writePaymentEvent(ShardJdbcContext context, PaymentRecord payment, GatewayEventType type, Map<String, String> metadata) {
        GatewayEvent event = new GatewayEvent(
                UUID.randomUUID(),
                type,
                "PAYMENT",
                payment.id().toString(),
                payment.merchantId(),
                payment.amountMinor(),
                payment.currency(),
                Instant.now(),
                metadata);
        paymentRepository.appendPaymentEvent(context, payment.id(), payment.merchantId(), type.name(), payment.status(), JsonUtils.toJson(metadata));
        paymentRepository.appendOutboxEvent(
                context, event.eventId(), event.aggregateType(), payment.id(), payment.merchantId(), type.name(), JsonUtils.toJson(event));
    }

    private void writeRefundEvent(ShardJdbcContext context, RefundRecord refund, GatewayEventType type, Map<String, String> metadata) {
        GatewayEvent event = new GatewayEvent(
                UUID.randomUUID(),
                type,
                "REFUND",
                refund.id().toString(),
                refund.merchantId(),
                refund.amountMinor(),
                refund.currency(),
                Instant.now(),
                metadata);
        paymentRepository.appendRefundEvent(
                context, refund.id(), refund.paymentId(), refund.merchantId(), type.name(), refund.status(), JsonUtils.toJson(metadata));
        paymentRepository.appendOutboxEvent(
                context, event.eventId(), event.aggregateType(), refund.id(), refund.merchantId(), type.name(), JsonUtils.toJson(event));
    }

    private PaymentResponse toPaymentResponse(ShardJdbcContext context, PaymentRecord payment) {
        return new PaymentResponse(
                payment.id(),
                payment.merchantId(),
                payment.merchantReference(),
                payment.amountMinor(),
                payment.currency(),
                payment.payerRef(),
                payment.payeeRef(),
                payment.status().name(),
                paymentRepository.sumSuccessfulRefundAmount(context, payment.id()),
                payment.lastError(),
                payment.createdAt(),
                payment.updatedAt());
    }

    private RefundResponse toRefundResponse(RefundRecord refund) {
        return new RefundResponse(
                refund.id(),
                refund.paymentId(),
                refund.merchantId(),
                refund.amountMinor(),
                refund.currency(),
                refund.status().name(),
                refund.lastError(),
                refund.createdAt(),
                refund.updatedAt());
    }
}
