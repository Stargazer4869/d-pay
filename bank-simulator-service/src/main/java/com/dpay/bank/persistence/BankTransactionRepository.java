package com.dpay.bank.persistence;

import com.dpay.bank.domain.BankOperationType;
import com.dpay.bank.domain.BankTransaction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class BankTransactionRepository {

    private static final RowMapper<BankTransaction> ROW_MAPPER = new RowMapper<>() {
        @Override
        public BankTransaction mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new BankTransaction(
                    rs.getString("operation_key"),
                    BankOperationType.valueOf(rs.getString("operation_type")),
                    rs.getString("account_ref"),
                    rs.getLong("amount_minor"),
                    rs.getString("currency"),
                    rs.getString("reference"),
                    rs.getString("status"),
                    rs.getInt("attempt_count"),
                    rs.getInt("failure_budget"),
                    rs.getBoolean("terminal"),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant());
        }
    };

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public BankTransactionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public BankTransaction findByOperationKey(String operationKey) {
        List<BankTransaction> transactions = jdbcTemplate.query(
                """
                select operation_key, operation_type, account_ref, amount_minor, currency, reference, status,
                       attempt_count, failure_budget, terminal, created_at, updated_at
                from bank.transactions
                where operation_key = :operationKey
                """,
                Map.of("operationKey", operationKey),
                ROW_MAPPER);
        return transactions.isEmpty() ? null : transactions.get(0);
    }

    public void insert(BankTransaction transaction) {
        jdbcTemplate.update(
                """
                insert into bank.transactions (
                    operation_key,
                    operation_type,
                    account_ref,
                    amount_minor,
                    currency,
                    reference,
                    status,
                    attempt_count,
                    failure_budget,
                    terminal,
                    created_at,
                    updated_at
                )
                values (
                    :operationKey,
                    :operationType,
                    :accountRef,
                    :amountMinor,
                    :currency,
                    :reference,
                    :status,
                    :attemptCount,
                    :failureBudget,
                    :terminal,
                    :createdAt,
                    :updatedAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("operationKey", transaction.operationKey())
                        .addValue("operationType", transaction.operationType().name())
                        .addValue("accountRef", transaction.accountRef())
                        .addValue("amountMinor", transaction.amountMinor())
                        .addValue("currency", transaction.currency())
                        .addValue("reference", transaction.reference())
                        .addValue("status", transaction.status())
                        .addValue("attemptCount", transaction.attemptCount())
                        .addValue("failureBudget", transaction.failureBudget())
                        .addValue("terminal", transaction.terminal())
                        .addValue("createdAt", transaction.createdAt())
                        .addValue("updatedAt", transaction.updatedAt()));
    }

    public void updateStatus(String operationKey, String status, int attemptCount, boolean terminal) {
        jdbcTemplate.update(
                """
                update bank.transactions
                set status = :status,
                    attempt_count = :attemptCount,
                    terminal = :terminal,
                    updated_at = :updatedAt
                where operation_key = :operationKey
                """,
                new MapSqlParameterSource()
                        .addValue("status", status)
                        .addValue("attemptCount", attemptCount)
                        .addValue("terminal", terminal)
                        .addValue("updatedAt", Instant.now())
                        .addValue("operationKey", operationKey));
    }
}
