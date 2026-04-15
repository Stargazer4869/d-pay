package com.dpay.transfer.persistence;

import com.dpay.transfer.domain.TransferRecord;
import com.dpay.transfer.domain.TransferStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class TransferRepository {

    private static final RowMapper<TransferRecord> TRANSFER_ROW_MAPPER = new RowMapper<>() {
        @Override
        public TransferRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new TransferRecord(
                    rs.getObject("id", UUID.class),
                    rs.getString("source_merchant_id"),
                    rs.getString("destination_merchant_id"),
                    rs.getString("merchant_reference"),
                    rs.getLong("amount_minor"),
                    rs.getString("currency"),
                    TransferStatus.valueOf(rs.getString("status")),
                    rs.getString("source_shard_id"),
                    rs.getString("destination_shard_id"),
                    rs.getInt("attempt_count"),
                    rs.getTimestamp("next_retry_at").toInstant(),
                    rs.getString("last_error"),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant());
        }
    };

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public TransferRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String locateShardId(String merchantId) {
        List<String> shardIds = jdbcTemplate.query(
                """
                select shard_id
                from platform.merchant_registry
                where merchant_id = :merchantId
                """,
                Map.of("merchantId", merchantId),
                (rs, rowNum) -> rs.getString("shard_id"));
        if (shardIds.isEmpty()) {
            throw new IllegalArgumentException("Unknown merchant: " + merchantId);
        }
        return shardIds.get(0);
    }

    public String findIdempotentResponse(String sourceMerchantId, String operation, String idempotencyKey) {
        List<String> responses = jdbcTemplate.query(
                """
                select response_body
                from transfer.idempotency_records
                where source_merchant_id = :sourceMerchantId
                  and operation = :operation
                  and idempotency_key = :idempotencyKey
                """,
                Map.of(
                        "sourceMerchantId", sourceMerchantId,
                        "operation", operation,
                        "idempotencyKey", idempotencyKey),
                (rs, rowNum) -> rs.getString("response_body"));
        return responses.isEmpty() ? null : responses.get(0);
    }

    public void saveIdempotencyResponse(
            String sourceMerchantId, String operation, String idempotencyKey, String responseBody) {
        jdbcTemplate.update(
                """
                insert into transfer.idempotency_records (
                    source_merchant_id,
                    operation,
                    idempotency_key,
                    response_body,
                    created_at
                )
                values (:sourceMerchantId, :operation, :idempotencyKey, cast(:responseBody as jsonb), :createdAt)
                """,
                new MapSqlParameterSource()
                        .addValue("sourceMerchantId", sourceMerchantId)
                        .addValue("operation", operation)
                        .addValue("idempotencyKey", idempotencyKey)
                        .addValue("responseBody", responseBody)
                        .addValue("createdAt", Instant.now()));
    }

    public void insertTransfer(TransferRecord transfer) {
        jdbcTemplate.update(
                """
                insert into transfer.transfers (
                    id,
                    source_merchant_id,
                    destination_merchant_id,
                    merchant_reference,
                    amount_minor,
                    currency,
                    status,
                    source_shard_id,
                    destination_shard_id,
                    attempt_count,
                    next_retry_at,
                    last_error,
                    created_at,
                    updated_at
                )
                values (
                    :id,
                    :sourceMerchantId,
                    :destinationMerchantId,
                    :merchantReference,
                    :amountMinor,
                    :currency,
                    :status,
                    :sourceShardId,
                    :destinationShardId,
                    :attemptCount,
                    :nextRetryAt,
                    :lastError,
                    :createdAt,
                    :updatedAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("id", transfer.id())
                        .addValue("sourceMerchantId", transfer.sourceMerchantId())
                        .addValue("destinationMerchantId", transfer.destinationMerchantId())
                        .addValue("merchantReference", transfer.merchantReference())
                        .addValue("amountMinor", transfer.amountMinor())
                        .addValue("currency", transfer.currency())
                        .addValue("status", transfer.status().name())
                        .addValue("sourceShardId", transfer.sourceShardId())
                        .addValue("destinationShardId", transfer.destinationShardId())
                        .addValue("attemptCount", transfer.attemptCount())
                        .addValue("nextRetryAt", transfer.nextRetryAt())
                        .addValue("lastError", transfer.lastError())
                        .addValue("createdAt", transfer.createdAt())
                        .addValue("updatedAt", transfer.updatedAt()));
    }

    public TransferRecord findTransfer(UUID id) {
        List<TransferRecord> transfers = jdbcTemplate.query(
                """
                select *
                from transfer.transfers
                where id = :id
                """,
                Map.of("id", id),
                TRANSFER_ROW_MAPPER);
        return transfers.isEmpty() ? null : transfers.get(0);
    }

    public List<TransferRecord> findTransfersForMerchant(String merchantId) {
        return jdbcTemplate.query(
                """
                select *
                from transfer.transfers
                where source_merchant_id = :merchantId
                   or destination_merchant_id = :merchantId
                order by created_at desc
                """,
                Map.of("merchantId", merchantId),
                TRANSFER_ROW_MAPPER);
    }

    public List<Map<String, Object>> findTransferEvents(UUID transferId) {
        return jdbcTemplate.queryForList(
                """
                select event_type, status, details::text as details, created_at
                from transfer.transfer_events
                where transfer_id = :transferId
                order by created_at asc
                """,
                Map.of("transferId", transferId));
    }

    public void appendEvent(UUID transferId, String eventType, TransferStatus status, String details) {
        jdbcTemplate.update(
                """
                insert into transfer.transfer_events (transfer_id, event_type, status, details, created_at)
                values (:transferId, :eventType, :status, cast(:details as jsonb), :createdAt)
                """,
                new MapSqlParameterSource()
                        .addValue("transferId", transferId)
                        .addValue("eventType", eventType)
                        .addValue("status", status.name())
                        .addValue("details", details)
                        .addValue("createdAt", Instant.now()));
    }

    public void appendOutboxEvent(UUID eventId, String aggregateType, UUID aggregateId, String eventType,
            String merchantId, String payload) {
        jdbcTemplate.update(
                """
                insert into transfer.outbox_events (
                    id, aggregate_type, aggregate_id, merchant_id, event_type, payload, published, created_at
                )
                values (
                    :id, :aggregateType, :aggregateId, :merchantId, :eventType, cast(:payload as jsonb), false, :createdAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("id", eventId)
                        .addValue("aggregateType", aggregateType)
                        .addValue("aggregateId", aggregateId.toString())
                        .addValue("merchantId", merchantId)
                        .addValue("eventType", eventType)
                        .addValue("payload", payload)
                        .addValue("createdAt", Instant.now()));
    }

    public List<Map<String, Object>> findPendingOutboxEvents(int limit) {
        return jdbcTemplate.queryForList(
                """
                select id, aggregate_id, event_type, payload::text as payload
                from transfer.outbox_events
                where published = false
                order by created_at asc
                limit :limit
                """,
                Map.of("limit", limit));
    }

    public void markOutboxPublished(UUID eventId) {
        jdbcTemplate.update(
                """
                update transfer.outbox_events
                set published = true,
                    published_at = :publishedAt
                where id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("publishedAt", Instant.now())
                        .addValue("id", eventId));
    }

    public void updateTransferState(UUID transferId, TransferStatus status, String lastError,
            Instant nextRetryAt, int attemptCount) {
        jdbcTemplate.update(
                """
                update transfer.transfers
                set status = :status,
                    last_error = :lastError,
                    next_retry_at = :nextRetryAt,
                    attempt_count = :attemptCount,
                    updated_at = :updatedAt
                where id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("status", status.name())
                        .addValue("lastError", lastError)
                        .addValue("nextRetryAt", nextRetryAt)
                        .addValue("attemptCount", attemptCount)
                        .addValue("updatedAt", Instant.now())
                        .addValue("id", transferId));
    }

    public List<TransferRecord> findDueTransfers(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                select *
                from transfer.transfers
                where status in ('REQUESTED', 'FUNDS_RESERVED', 'CREDITING', 'COMPENSATING')
                  and next_retry_at <= :now
                order by next_retry_at asc
                limit :limit
                """,
                Map.of("now", now, "limit", limit),
                TRANSFER_ROW_MAPPER);
    }
}
