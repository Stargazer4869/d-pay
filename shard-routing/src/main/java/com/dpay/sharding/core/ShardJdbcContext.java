package com.dpay.sharding.core;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public record ShardJdbcContext(
        String shardId,
        JdbcTemplate jdbcTemplate,
        NamedParameterJdbcTemplate namedJdbcTemplate,
        TransactionTemplate transactionTemplate
) {
}
