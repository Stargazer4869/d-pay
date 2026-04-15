package com.dpay.transfer.app;

import com.dpay.common.api.OperationResult;
import com.dpay.common.api.TransferCreditRequest;
import com.dpay.common.api.TransferReserveRequest;
import com.dpay.common.events.GatewayEvent;
import com.dpay.common.events.GatewayEventType;
import com.dpay.common.json.JsonUtils;
import com.dpay.transfer.api.CreateTransferRequest;
import com.dpay.transfer.api.TransferEventResponse;
import com.dpay.transfer.api.TransferResponse;
import com.dpay.transfer.domain.TransferRecord;
import com.dpay.transfer.domain.TransferStatus;
import com.dpay.transfer.integration.LedgerHttpClient;
import com.dpay.transfer.persistence.TransferRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class TransferService {

    private static final String CREATE_TRANSFER_OPERATION = "CREATE_TRANSFER";

    private final TransferRepository transferRepository;
    private final LedgerHttpClient ledgerHttpClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final String topic;
    private final int maxAttempts;
    private final Duration retryDelay;

    public TransferService(
            TransferRepository transferRepository,
            LedgerHttpClient ledgerHttpClient,
            KafkaTemplate<String, String> kafkaTemplate,
            TransactionTemplate transactionTemplate,
            @Value("${app.kafka.topic:gateway-events}") String topic,
            @Value("${app.saga.max-attempts:3}") int maxAttempts,
            @Value("${app.saga.retry-delay-seconds:2}") long retryDelaySeconds) {
        this.transferRepository = transferRepository;
        this.ledgerHttpClient = ledgerHttpClient;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.topic = topic;
        this.maxAttempts = maxAttempts;
        this.retryDelay = Duration.ofSeconds(retryDelaySeconds);
    }

    @Transactional
    public TransferResponse createTransfer(CreateTransferRequest request, String idempotencyKey) {
        String cached = transferRepository.findIdempotentResponse(
                request.sourceMerchantId(), CREATE_TRANSFER_OPERATION, idempotencyKey);
        if (cached != null) {
            return JsonUtils.fromJson(cached, TransferResponse.class);
        }

        String sourceShardId = transferRepository.locateShardId(request.sourceMerchantId());
        String destinationShardId = transferRepository.locateShardId(request.destinationMerchantId());
        Instant now = Instant.now();
        TransferRecord transfer = new TransferRecord(
                UUID.randomUUID(),
                request.sourceMerchantId(),
                request.destinationMerchantId(),
                request.merchantReference(),
                request.amountMinor(),
                request.currency(),
                TransferStatus.REQUESTED,
                sourceShardId,
                destinationShardId,
                0,
                now,
                null,
                now,
                now);
        transferRepository.insertTransfer(transfer);
        writeTransferEvent(transfer, GatewayEventType.TRANSFER_REQUESTED, Map.of(
                "merchantReference", request.merchantReference(),
                "sourceShardId", sourceShardId,
                "destinationShardId", destinationShardId));
        TransferResponse response = toResponse(transfer);
        transferRepository.saveIdempotencyResponse(
                request.sourceMerchantId(), CREATE_TRANSFER_OPERATION, idempotencyKey, JsonUtils.toJson(response));
        return response;
    }

    public TransferResponse getTransfer(UUID transferId) {
        return toResponse(requiredTransfer(transferId));
    }

    public List<TransferResponse> getTransfers(String merchantId) {
        return transferRepository.findTransfersForMerchant(merchantId)
                .stream()
                .map(this::toResponse)
                .toList();
    }

    public List<TransferEventResponse> getHistory(UUID transferId) {
        return transferRepository.findTransferEvents(transferId).stream()
                .map(row -> new TransferEventResponse(
                        row.get("event_type").toString(),
                        row.get("status").toString(),
                        row.get("details").toString(),
                        ((java.sql.Timestamp) row.get("created_at")).toInstant()))
                .toList();
    }

    public void publishPendingOutbox() {
        transferRepository.findPendingOutboxEvents(100).forEach(event -> {
            UUID eventId = UUID.fromString(event.get("id").toString());
            kafkaTemplate.send(topic, event.get("aggregate_id").toString(), event.get("payload").toString());
            transferRepository.markOutboxPublished(eventId);
        });
    }

    public void reconcileTransfers() {
        transferRepository.findDueTransfers(Instant.now(), 100).forEach(transfer -> {
            try {
                advanceTransfer(transfer.id());
            } catch (Exception exception) {
                // the next retry has already been scheduled by advanceTransfer
            }
        });
    }

    public void advanceTransfer(UUID transferId) {
        TransferRecord transfer = requiredTransfer(transferId);
        switch (transfer.status()) {
            case REQUESTED -> handleRequested(transfer);
            case FUNDS_RESERVED -> transitionState(transfer, TransferStatus.CREDITING, null);
            case CREDITING -> handleCrediting(transfer);
            case COMPENSATING -> handleCompensating(transfer);
            case COMPLETED, COMPENSATED, FAILED -> {
            }
        }
    }

    private void handleRequested(TransferRecord transfer) {
        try {
            OperationResult result = ledgerHttpClient.reserveTransfer(
                    new TransferReserveRequest(
                            transfer.sourceMerchantId(),
                            transfer.id().toString(),
                            transfer.amountMinor(),
                            transfer.currency()));
            if (!result.success()) {
                if (result.retryable()) {
                    scheduleRetry(transfer, result.message());
                    return;
                }
                failTransfer(transfer, result.message());
                return;
            }
            transitionState(transfer, TransferStatus.FUNDS_RESERVED, null);
        } catch (Exception exception) {
            scheduleRetry(transfer, exception.getMessage());
        }
    }

    private void handleCrediting(TransferRecord transfer) {
        try {
            OperationResult creditResult = ledgerHttpClient.creditTransfer(
                    new TransferCreditRequest(
                            transfer.destinationMerchantId(),
                            transfer.id().toString(),
                            transfer.amountMinor(),
                            transfer.currency()));
            if (!creditResult.success()) {
                if (creditResult.retryable()) {
                    scheduleRetry(transfer, creditResult.message());
                    return;
                }
                beginCompensation(transfer, creditResult.message());
                return;
            }

            OperationResult finalizeResult = ledgerHttpClient.finalizeTransfer(
                    new TransferReserveRequest(
                            transfer.sourceMerchantId(),
                            transfer.id().toString(),
                            transfer.amountMinor(),
                            transfer.currency()));
            if (!finalizeResult.success()) {
                beginCompensation(transfer, finalizeResult.message());
                return;
            }

            transitionState(transfer, TransferStatus.COMPLETED, null);
        } catch (Exception exception) {
            if (transfer.attemptCount() + 1 >= maxAttempts) {
                beginCompensation(transfer, exception.getMessage());
                return;
            }
            scheduleRetry(transfer, exception.getMessage());
        }
    }

    private void handleCompensating(TransferRecord transfer) {
        try {
            OperationResult releaseResult = ledgerHttpClient.releaseTransfer(
                    new TransferReserveRequest(
                            transfer.sourceMerchantId(),
                            transfer.id().toString(),
                            transfer.amountMinor(),
                            transfer.currency()));
            if (!releaseResult.success()) {
                scheduleRetry(transfer, releaseResult.message());
                return;
            }
            transitionState(transfer, TransferStatus.COMPENSATED, transfer.lastError());
        } catch (Exception exception) {
            scheduleRetry(transfer, exception.getMessage());
        }
    }

    private void transitionState(TransferRecord transfer, TransferStatus status, String lastError) {
        Instant nextRetryAt = status == TransferStatus.COMPLETED || status == TransferStatus.COMPENSATED || status == TransferStatus.FAILED
                ? Instant.now()
                : Instant.now().plus(retryDelay);
        int attemptCount = status == TransferStatus.REQUESTED || status == TransferStatus.FUNDS_RESERVED || status == TransferStatus.CREDITING
                ? 0
                : transfer.attemptCount();
        TransferRecord refreshed = transactionTemplate.execute(statusTx -> {
            transferRepository.updateTransferState(transfer.id(), status, lastError, nextRetryAt, attemptCount);
            TransferRecord updated = requiredTransfer(transfer.id());
            writeTransferEvent(updated, mapEventType(status), Map.of("lastError", String.valueOf(lastError)));
            return updated;
        });
        if (status == TransferStatus.FUNDS_RESERVED) {
            advanceTransfer(transfer.id());
        } else if (status == TransferStatus.CREDITING) {
            advanceTransfer(transfer.id());
        } else if (status == TransferStatus.COMPENSATING) {
            advanceTransfer(transfer.id());
        }
    }

    private void failTransfer(TransferRecord transfer, String errorMessage) {
        transactionTemplate.executeWithoutResult(status -> {
            transferRepository.updateTransferState(
                    transfer.id(),
                    TransferStatus.FAILED,
                    errorMessage,
                    Instant.now(),
                    transfer.attemptCount());
            writeTransferEvent(requiredTransfer(transfer.id()), GatewayEventType.TRANSFER_FAILED, Map.of("lastError", errorMessage));
        });
    }

    private void beginCompensation(TransferRecord transfer, String errorMessage) {
        transactionTemplate.executeWithoutResult(status -> {
            transferRepository.updateTransferState(
                    transfer.id(),
                    TransferStatus.COMPENSATING,
                    errorMessage,
                    Instant.now().plus(retryDelay),
                    transfer.attemptCount());
            writeTransferEvent(requiredTransfer(transfer.id()), GatewayEventType.TRANSFER_COMPENSATING, Map.of("lastError", errorMessage));
        });
        advanceTransfer(transfer.id());
    }

    private void scheduleRetry(TransferRecord transfer, String errorMessage) {
        int attemptCount = transfer.attemptCount() + 1;
        TransferStatus nextStatus = transfer.status();
        if (attemptCount >= maxAttempts && transfer.status() == TransferStatus.REQUESTED) {
            failTransfer(transfer, errorMessage);
            return;
        }
        if (attemptCount >= maxAttempts && (transfer.status() == TransferStatus.FUNDS_RESERVED || transfer.status() == TransferStatus.CREDITING)) {
            beginCompensation(transfer, errorMessage);
            return;
        }
        transactionTemplate.executeWithoutResult(status -> transferRepository.updateTransferState(
                transfer.id(),
                nextStatus,
                errorMessage,
                Instant.now().plus(retryDelay),
                attemptCount));
    }

    private void writeTransferEvent(TransferRecord transfer, GatewayEventType type, Map<String, String> metadata) {
        GatewayEvent event = new GatewayEvent(
                UUID.randomUUID(),
                type,
                "TRANSFER",
                transfer.id().toString(),
                transfer.sourceMerchantId(),
                transfer.amountMinor(),
                transfer.currency(),
                Instant.now(),
                Map.of(
                        "sourceMerchantId", transfer.sourceMerchantId(),
                        "destinationMerchantId", transfer.destinationMerchantId(),
                        "status", transfer.status().name(),
                        "merchantReference", transfer.merchantReference(),
                        "lastError", transfer.lastError() == null ? "" : transfer.lastError(),
                        "details", JsonUtils.toJson(metadata)));
        transferRepository.appendEvent(transfer.id(), type.name(), transfer.status(), JsonUtils.toJson(event.metadata()));
        transferRepository.appendOutboxEvent(
                event.eventId(),
                event.aggregateType(),
                transfer.id(),
                type.name(),
                transfer.sourceMerchantId(),
                JsonUtils.toJson(event));
    }

    private GatewayEventType mapEventType(TransferStatus status) {
        return switch (status) {
            case REQUESTED -> GatewayEventType.TRANSFER_REQUESTED;
            case FUNDS_RESERVED -> GatewayEventType.TRANSFER_FUNDS_RESERVED;
            case CREDITING -> GatewayEventType.TRANSFER_CREDITING;
            case COMPLETED -> GatewayEventType.TRANSFER_COMPLETED;
            case COMPENSATING -> GatewayEventType.TRANSFER_COMPENSATING;
            case COMPENSATED -> GatewayEventType.TRANSFER_COMPENSATED;
            case FAILED -> GatewayEventType.TRANSFER_FAILED;
        };
    }

    private TransferRecord requiredTransfer(UUID transferId) {
        TransferRecord transfer = transferRepository.findTransfer(transferId);
        if (transfer == null) {
            throw new IllegalArgumentException("Unknown transfer " + transferId);
        }
        return transfer;
    }

    private TransferResponse toResponse(TransferRecord transfer) {
        return new TransferResponse(
                transfer.id(),
                transfer.sourceMerchantId(),
                transfer.destinationMerchantId(),
                transfer.merchantReference(),
                transfer.amountMinor(),
                transfer.currency(),
                transfer.status().name(),
                transfer.lastError(),
                transfer.createdAt(),
                transfer.updatedAt());
    }
}
