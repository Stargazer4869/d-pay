package com.dpay.ledger.persistence;

import com.dpay.ledger.domain.LedgerAccount;
import com.dpay.ledger.domain.LedgerOperationType;
import com.dpay.sharding.core.ShardJdbcContext;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Repository;

@Repository
public class LedgerRepository {

    private static final RowMapper<LedgerAccount> ACCOUNT_ROW_MAPPER = new RowMapper<>() {
        @Override
        public LedgerAccount mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new LedgerAccount(
                    rs.getString("merchant_id"),
                    rs.getString("currency"),
                    rs.getLong("available_balance"),
                    rs.getLong("reserved_outgoing"),
                    rs.getLong("reserved_refund"));
        }
    };

    public void ensureAccount(ShardJdbcContext context, String merchantId, String currency) {
        context.namedJdbcTemplate().update(
                """
                insert into ledger.accounts (merchant_id, currency, available_balance, reserved_outgoing, reserved_refund, updated_at)
                values (:merchantId, :currency, 0, 0, 0, :updatedAt)
                on conflict (merchant_id, currency) do nothing
                """,
                new MapSqlParameterSource()
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency)
                        .addValue("updatedAt", Instant.now()));
    }

    public boolean isOperationApplied(ShardJdbcContext context, String merchantId, String currency,
            LedgerOperationType operationType, String operationId) {
        Integer count = context.namedJdbcTemplate().queryForObject(
                """
                select count(*)
                from ledger.operation_log
                where merchant_id = :merchantId
                  and currency = :currency
                  and operation_type = :operationType
                  and operation_id = :operationId
                """,
                Map.of(
                        "merchantId", merchantId,
                        "currency", currency,
                        "operationType", operationType.name(),
                        "operationId", operationId),
                Integer.class);
        return count != null && count > 0;
    }

    public void markOperationApplied(ShardJdbcContext context, String merchantId, String currency,
            LedgerOperationType operationType, String operationId) {
        context.namedJdbcTemplate().update(
                """
                insert into ledger.operation_log (merchant_id, currency, operation_type, operation_id, created_at)
                values (:merchantId, :currency, :operationType, :operationId, :createdAt)
                """,
                new MapSqlParameterSource()
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency)
                        .addValue("operationType", operationType.name())
                        .addValue("operationId", operationId)
                        .addValue("createdAt", Instant.now()));
    }

    public void insertJournalEntry(ShardJdbcContext context, String merchantId, String currency, String operationType,
            String operationId, String balanceBucket, long amountDelta, String details) {
        context.namedJdbcTemplate().update(
                """
                insert into ledger.journal_entries (
                    id,
                    merchant_id,
                    currency,
                    operation_type,
                    operation_id,
                    balance_bucket,
                    amount_delta,
                    details,
                    created_at
                )
                values (
                    :id,
                    :merchantId,
                    :currency,
                    :operationType,
                    :operationId,
                    :balanceBucket,
                    :amountDelta,
                    cast(:details as jsonb),
                    :createdAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("id", UUID.randomUUID())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency)
                        .addValue("operationType", operationType)
                        .addValue("operationId", operationId)
                        .addValue("balanceBucket", balanceBucket)
                        .addValue("amountDelta", amountDelta)
                        .addValue("details", details)
                        .addValue("createdAt", Instant.now()));
    }

    public int updateBalance(ShardJdbcContext context, String merchantId, String currency, long availableDelta,
            long reservedOutgoingDelta, long reservedRefundDelta) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set available_balance = available_balance + :availableDelta,
                    reserved_outgoing = reserved_outgoing + :reservedOutgoingDelta,
                    reserved_refund = reserved_refund + :reservedRefundDelta,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                """,
                new MapSqlParameterSource()
                        .addValue("availableDelta", availableDelta)
                        .addValue("reservedOutgoingDelta", reservedOutgoingDelta)
                        .addValue("reservedRefundDelta", reservedRefundDelta)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public int reserveOutgoing(ShardJdbcContext context, String merchantId, String currency, long amountMinor) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set available_balance = available_balance - :amountMinor,
                    reserved_outgoing = reserved_outgoing + :amountMinor,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                  and available_balance >= :amountMinor
                """,
                new MapSqlParameterSource()
                        .addValue("amountMinor", amountMinor)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public int finalizeOutgoing(ShardJdbcContext context, String merchantId, String currency, long amountMinor) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set reserved_outgoing = reserved_outgoing - :amountMinor,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                  and reserved_outgoing >= :amountMinor
                """,
                new MapSqlParameterSource()
                        .addValue("amountMinor", amountMinor)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public int releaseOutgoing(ShardJdbcContext context, String merchantId, String currency, long amountMinor) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set available_balance = available_balance + :amountMinor,
                    reserved_outgoing = reserved_outgoing - :amountMinor,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                  and reserved_outgoing >= :amountMinor
                """,
                new MapSqlParameterSource()
                        .addValue("amountMinor", amountMinor)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public int reserveRefund(ShardJdbcContext context, String merchantId, String currency, long amountMinor) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set available_balance = available_balance - :amountMinor,
                    reserved_refund = reserved_refund + :amountMinor,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                  and available_balance >= :amountMinor
                """,
                new MapSqlParameterSource()
                        .addValue("amountMinor", amountMinor)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public int finalizeRefund(ShardJdbcContext context, String merchantId, String currency, long amountMinor) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set reserved_refund = reserved_refund - :amountMinor,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                  and reserved_refund >= :amountMinor
                """,
                new MapSqlParameterSource()
                        .addValue("amountMinor", amountMinor)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public int releaseRefund(ShardJdbcContext context, String merchantId, String currency, long amountMinor) {
        return context.namedJdbcTemplate().update(
                """
                update ledger.accounts
                set available_balance = available_balance + :amountMinor,
                    reserved_refund = reserved_refund - :amountMinor,
                    updated_at = :updatedAt
                where merchant_id = :merchantId
                  and currency = :currency
                  and reserved_refund >= :amountMinor
                """,
                new MapSqlParameterSource()
                        .addValue("amountMinor", amountMinor)
                        .addValue("updatedAt", Instant.now())
                        .addValue("merchantId", merchantId)
                        .addValue("currency", currency));
    }

    public LedgerAccount findAccount(ShardJdbcContext context, String merchantId, String currency) {
        return context.namedJdbcTemplate().query(
                        """
                        select merchant_id, currency, available_balance, reserved_outgoing, reserved_refund
                        from ledger.accounts
                        where merchant_id = :merchantId
                          and currency = :currency
                        """,
                        Map.of("merchantId", merchantId, "currency", currency),
                        ACCOUNT_ROW_MAPPER)
                .stream()
                .findFirst()
                .orElse(new LedgerAccount(merchantId, currency, 0, 0, 0));
    }
}
