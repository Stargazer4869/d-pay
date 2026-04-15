package com.dpay.notification.persistence;

import com.dpay.notification.domain.WebhookDeliveryRecord;
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
public class NotificationRepository {

    private static final RowMapper<WebhookDeliveryRecord> DELIVERY_ROW_MAPPER = new RowMapper<>() {
        @Override
        public WebhookDeliveryRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new WebhookDeliveryRecord(
                    rs.getObject("event_id", UUID.class),
                    rs.getString("merchant_id"),
                    rs.getString("target_url"),
                    rs.getString("secret"),
                    rs.getString("payload"),
                    rs.getString("status"),
                    rs.getInt("attempt_count"),
                    (Integer) rs.getObject("last_response_code"),
                    rs.getString("last_error"),
                    rs.getTimestamp("next_attempt_at").toInstant());
        }
    };

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public NotificationRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void upsertSubscription(String merchantId, String targetUrl, String secret, boolean active) {
        jdbcTemplate.update(
                """
                insert into platform.webhook_subscriptions (merchant_id, target_url, secret, active, created_at, updated_at)
                values (:merchantId, :targetUrl, :secret, :active, :createdAt, :updatedAt)
                on conflict (merchant_id) do update
                set target_url = excluded.target_url,
                    secret = excluded.secret,
                    active = excluded.active,
                    updated_at = excluded.updated_at
                """,
                new MapSqlParameterSource()
                        .addValue("merchantId", merchantId)
                        .addValue("targetUrl", targetUrl)
                        .addValue("secret", secret)
                        .addValue("active", active)
                        .addValue("createdAt", Instant.now())
                        .addValue("updatedAt", Instant.now()));
    }

    public List<Map<String, Object>> findActiveSubscriptions(String merchantId) {
        return jdbcTemplate.queryForList(
                """
                select merchant_id, target_url, secret
                from platform.webhook_subscriptions
                where merchant_id = :merchantId
                  and active = true
                """,
                Map.of("merchantId", merchantId));
    }

    public boolean isEventProcessed(UUID eventId) {
        Integer count = jdbcTemplate.queryForObject(
                """
                select count(*)
                from notification.inbox_events
                where event_id = :eventId
                """,
                Map.of("eventId", eventId),
                Integer.class);
        return count != null && count > 0;
    }

    public void markEventProcessed(UUID eventId) {
        jdbcTemplate.update(
                """
                insert into notification.inbox_events (event_id, processed_at)
                values (:eventId, :processedAt)
                on conflict do nothing
                """,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("processedAt", Instant.now()));
    }

    public void upsertDelivery(UUID eventId, String merchantId, String targetUrl, String secret, String payload) {
        jdbcTemplate.update(
                """
                insert into notification.webhook_deliveries (
                    event_id, merchant_id, target_url, secret, payload, status, attempt_count,
                    created_at, updated_at, next_attempt_at
                )
                values (
                    :eventId, :merchantId, :targetUrl, :secret, :payload, 'PENDING', 0, :createdAt, :updatedAt, :nextAttemptAt
                )
                on conflict (event_id, merchant_id, target_url) do update
                set secret = excluded.secret,
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("merchantId", merchantId)
                        .addValue("targetUrl", targetUrl)
                        .addValue("secret", secret)
                        .addValue("payload", payload)
                        .addValue("createdAt", Instant.now())
                        .addValue("updatedAt", Instant.now())
                        .addValue("nextAttemptAt", Instant.now()));
    }

    public List<WebhookDeliveryRecord> findDueDeliveries(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                select event_id, merchant_id, target_url, secret, payload, status, attempt_count,
                       last_response_code, last_error, next_attempt_at
                from notification.webhook_deliveries
                where status = 'PENDING'
                  and next_attempt_at <= :now
                order by next_attempt_at asc
                limit :limit
                """,
                Map.of("now", now, "limit", limit),
                DELIVERY_ROW_MAPPER);
    }

    public void updateDelivery(UUID eventId, String merchantId, String targetUrl, String status, int attemptCount,
            Integer responseCode, String lastError, Instant nextAttemptAt) {
        jdbcTemplate.update(
                """
                update notification.webhook_deliveries
                set status = :status,
                    attempt_count = :attemptCount,
                    last_response_code = :responseCode,
                    last_error = :lastError,
                    next_attempt_at = :nextAttemptAt,
                    updated_at = :updatedAt
                where event_id = :eventId
                  and merchant_id = :merchantId
                  and target_url = :targetUrl
                """,
                new MapSqlParameterSource()
                        .addValue("status", status)
                        .addValue("attemptCount", attemptCount)
                        .addValue("responseCode", responseCode)
                        .addValue("lastError", lastError)
                        .addValue("nextAttemptAt", nextAttemptAt)
                        .addValue("updatedAt", Instant.now())
                        .addValue("eventId", eventId)
                        .addValue("merchantId", merchantId)
                        .addValue("targetUrl", targetUrl));
    }
}
