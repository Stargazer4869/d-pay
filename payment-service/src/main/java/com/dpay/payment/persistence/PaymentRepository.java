package com.dpay.payment.persistence;

import com.dpay.payment.domain.PaymentRecord;
import com.dpay.payment.domain.PaymentStatus;
import com.dpay.payment.domain.RefundRecord;
import com.dpay.payment.domain.RefundStatus;
import com.dpay.sharding.core.ShardJdbcContext;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {

    private static final RowMapper<PaymentRecord> PAYMENT_ROW_MAPPER = new RowMapper<>() {
        @Override
        public PaymentRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new PaymentRecord(
                    rs.getObject("id", UUID.class),
                    rs.getString("merchant_id"),
                    rs.getString("merchant_reference"),
                    rs.getLong("amount_minor"),
                    rs.getString("currency"),
                    rs.getString("payer_ref"),
                    rs.getString("payee_ref"),
                    PaymentStatus.valueOf(rs.getString("status")),
                    rs.getInt("processing_attempts"),
                    rs.getString("last_error"),
                    rs.getTimestamp("expires_at") == null ? null : rs.getTimestamp("expires_at").toInstant(),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant());
        }
    };

    private static final RowMapper<RefundRecord> REFUND_ROW_MAPPER = new RowMapper<>() {
        @Override
        public RefundRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new RefundRecord(
                    rs.getObject("id", UUID.class),
                    rs.getObject("payment_id", UUID.class),
                    rs.getString("merchant_id"),
                    rs.getLong("amount_minor"),
                    rs.getString("currency"),
                    RefundStatus.valueOf(rs.getString("status")),
                    rs.getInt("processing_attempts"),
                    rs.getString("last_error"),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant());
        }
    };

    public String findIdempotentResponse(ShardJdbcContext context, String merchantId, String operation, String idempotencyKey) {
        List<String> responses = context.namedJdbcTemplate().query(
                """
                select response_body::text as response_body
                from payment.idempotency_records
                where merchant_id = :merchantId
                  and operation = :operation
                  and idempotency_key = :idempotencyKey
                """,
                Map.of("merchantId", merchantId, "operation", operation, "idempotencyKey", idempotencyKey),
                (rs, rowNum) -> rs.getString("response_body"));
        return responses.isEmpty() ? null : responses.get(0);
    }

    public void saveIdempotentResponse(
            ShardJdbcContext context, String merchantId, String operation, String idempotencyKey, String responseBody) {
        context.namedJdbcTemplate().update(
                """
                insert into payment.idempotency_records (
                    merchant_id, operation, idempotency_key, response_body, created_at
                )
                values (:merchantId, :operation, :idempotencyKey, cast(:responseBody as jsonb), :createdAt)
                """,
                new MapSqlParameterSource()
                        .addValue("merchantId", merchantId)
                        .addValue("operation", operation)
                        .addValue("idempotencyKey", idempotencyKey)
                        .addValue("responseBody", responseBody)
                        .addValue("createdAt", Instant.now()));
    }

    public void insertPayment(ShardJdbcContext context, PaymentRecord payment) {
        context.namedJdbcTemplate().update(
                """
                insert into payment.payments (
                    id, merchant_id, merchant_reference, amount_minor, currency, payer_ref, payee_ref, status,
                    processing_attempts, last_error, expires_at, created_at, updated_at
                )
                values (
                    :id, :merchantId, :merchantReference, :amountMinor, :currency, :payerRef, :payeeRef, :status,
                    :processingAttempts, :lastError, :expiresAt, :createdAt, :updatedAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("id", payment.id())
                        .addValue("merchantId", payment.merchantId())
                        .addValue("merchantReference", payment.merchantReference())
                        .addValue("amountMinor", payment.amountMinor())
                        .addValue("currency", payment.currency())
                        .addValue("payerRef", payment.payerRef())
                        .addValue("payeeRef", payment.payeeRef())
                        .addValue("status", payment.status().name())
                        .addValue("processingAttempts", payment.processingAttempts())
                        .addValue("lastError", payment.lastError())
                        .addValue("expiresAt", payment.expiresAt())
                        .addValue("createdAt", payment.createdAt())
                        .addValue("updatedAt", payment.updatedAt()));
    }

    public PaymentRecord findPayment(ShardJdbcContext context, UUID paymentId) {
        List<PaymentRecord> payments = context.namedJdbcTemplate().query(
                """
                select *
                from payment.payments
                where id = :id
                """,
                Map.of("id", paymentId),
                PAYMENT_ROW_MAPPER);
        return payments.isEmpty() ? null : payments.get(0);
    }

    public PaymentRecord findPaymentForUpdate(ShardJdbcContext context, UUID paymentId) {
        List<PaymentRecord> payments = context.namedJdbcTemplate().query(
                """
                select *
                from payment.payments
                where id = :id
                for update
                """,
                Map.of("id", paymentId),
                PAYMENT_ROW_MAPPER);
        return payments.isEmpty() ? null : payments.get(0);
    }

    public List<PaymentRecord> findPaymentsByMerchant(ShardJdbcContext context, String merchantId) {
        return context.namedJdbcTemplate().query(
                """
                select *
                from payment.payments
                where merchant_id = :merchantId
                order by created_at desc
                """,
                Map.of("merchantId", merchantId),
                PAYMENT_ROW_MAPPER);
    }

    public void updatePaymentState(ShardJdbcContext context, UUID paymentId, PaymentStatus status,
            int processingAttempts, String lastError) {
        context.namedJdbcTemplate().update(
                """
                update payment.payments
                set status = :status,
                    processing_attempts = :processingAttempts,
                    last_error = :lastError,
                    updated_at = :updatedAt
                where id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("status", status.name())
                        .addValue("processingAttempts", processingAttempts)
                        .addValue("lastError", lastError)
                        .addValue("updatedAt", Instant.now())
                        .addValue("id", paymentId));
    }

    public void appendPaymentEvent(ShardJdbcContext context, UUID paymentId, String merchantId, String eventType,
            PaymentStatus status, String details) {
        context.namedJdbcTemplate().update(
                """
                insert into payment.payment_events (payment_id, merchant_id, event_type, status, details, created_at)
                values (:paymentId, :merchantId, :eventType, :status, cast(:details as jsonb), :createdAt)
                """,
                new MapSqlParameterSource()
                        .addValue("paymentId", paymentId)
                        .addValue("merchantId", merchantId)
                        .addValue("eventType", eventType)
                        .addValue("status", status.name())
                        .addValue("details", details)
                        .addValue("createdAt", Instant.now()));
    }

    public RefundRecord findRefund(ShardJdbcContext context, UUID refundId) {
        List<RefundRecord> refunds = context.namedJdbcTemplate().query(
                """
                select *
                from payment.refunds
                where id = :id
                """,
                Map.of("id", refundId),
                REFUND_ROW_MAPPER);
        return refunds.isEmpty() ? null : refunds.get(0);
    }

    public RefundRecord findRefundForUpdate(ShardJdbcContext context, UUID refundId) {
        List<RefundRecord> refunds = context.namedJdbcTemplate().query(
                """
                select *
                from payment.refunds
                where id = :id
                for update
                """,
                Map.of("id", refundId),
                REFUND_ROW_MAPPER);
        return refunds.isEmpty() ? null : refunds.get(0);
    }

    public void insertRefund(ShardJdbcContext context, RefundRecord refund) {
        context.namedJdbcTemplate().update(
                """
                insert into payment.refunds (
                    id, payment_id, merchant_id, amount_minor, currency, status, processing_attempts, last_error,
                    created_at, updated_at
                )
                values (
                    :id, :paymentId, :merchantId, :amountMinor, :currency, :status, :processingAttempts, :lastError,
                    :createdAt, :updatedAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("id", refund.id())
                        .addValue("paymentId", refund.paymentId())
                        .addValue("merchantId", refund.merchantId())
                        .addValue("amountMinor", refund.amountMinor())
                        .addValue("currency", refund.currency())
                        .addValue("status", refund.status().name())
                        .addValue("processingAttempts", refund.processingAttempts())
                        .addValue("lastError", refund.lastError())
                        .addValue("createdAt", refund.createdAt())
                        .addValue("updatedAt", refund.updatedAt()));
    }

    public void updateRefundState(ShardJdbcContext context, UUID refundId, RefundStatus status,
            int processingAttempts, String lastError) {
        context.namedJdbcTemplate().update(
                """
                update payment.refunds
                set status = :status,
                    processing_attempts = :processingAttempts,
                    last_error = :lastError,
                    updated_at = :updatedAt
                where id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("status", status.name())
                        .addValue("processingAttempts", processingAttempts)
                        .addValue("lastError", lastError)
                        .addValue("updatedAt", Instant.now())
                        .addValue("id", refundId));
    }

    public void appendRefundEvent(ShardJdbcContext context, UUID refundId, UUID paymentId, String merchantId,
            String eventType, RefundStatus status, String details) {
        context.namedJdbcTemplate().update(
                """
                insert into payment.refund_events (refund_id, payment_id, merchant_id, event_type, status, details, created_at)
                values (:refundId, :paymentId, :merchantId, :eventType, :status, cast(:details as jsonb), :createdAt)
                """,
                new MapSqlParameterSource()
                        .addValue("refundId", refundId)
                        .addValue("paymentId", paymentId)
                        .addValue("merchantId", merchantId)
                        .addValue("eventType", eventType)
                        .addValue("status", status.name())
                        .addValue("details", details)
                        .addValue("createdAt", Instant.now()));
    }

    public long sumBlockedRefundAmount(ShardJdbcContext context, UUID paymentId) {
        Long amount = context.namedJdbcTemplate().queryForObject(
                """
                select coalesce(sum(amount_minor), 0)
                from payment.refunds
                where payment_id = :paymentId
                  and status in ('PROCESSING', 'SUCCEEDED')
                """,
                Map.of("paymentId", paymentId),
                Long.class);
        return amount == null ? 0L : amount;
    }

    public long sumSuccessfulRefundAmount(ShardJdbcContext context, UUID paymentId) {
        Long amount = context.namedJdbcTemplate().queryForObject(
                """
                select coalesce(sum(amount_minor), 0)
                from payment.refunds
                where payment_id = :paymentId
                  and status = 'SUCCEEDED'
                """,
                Map.of("paymentId", paymentId),
                Long.class);
        return amount == null ? 0L : amount;
    }

    public List<Map<String, Object>> findHistory(ShardJdbcContext context, UUID paymentId) {
        return context.namedJdbcTemplate().queryForList(
                """
                select 'PAYMENT' as resource_type,
                       payment_id::text as resource_id,
                       event_type,
                       status,
                       details::text as details,
                       created_at
                from payment.payment_events
                where payment_id = :paymentId
                union all
                select 'REFUND' as resource_type,
                       refund_id::text as resource_id,
                       event_type,
                       status,
                       details::text as details,
                       created_at
                from payment.refund_events
                where payment_id = :paymentId
                order by created_at asc
                """,
                Map.of("paymentId", paymentId));
    }

    public void appendOutboxEvent(ShardJdbcContext context, UUID eventId, String aggregateType, UUID aggregateId,
            String merchantId, String eventType, String payload) {
        context.namedJdbcTemplate().update(
                """
                insert into payment.outbox_events (
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

    public List<Map<String, Object>> findPendingOutboxEvents(ShardJdbcContext context, int limit) {
        return context.namedJdbcTemplate().queryForList(
                """
                select id, aggregate_id, event_type, payload::text as payload
                from payment.outbox_events
                where published = false
                order by created_at asc
                limit :limit
                """,
                Map.of("limit", limit));
    }

    public void markOutboxPublished(ShardJdbcContext context, UUID eventId) {
        context.namedJdbcTemplate().update(
                """
                update payment.outbox_events
                set published = true,
                    published_at = :publishedAt
                where id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("publishedAt", Instant.now())
                        .addValue("id", eventId));
    }
}
