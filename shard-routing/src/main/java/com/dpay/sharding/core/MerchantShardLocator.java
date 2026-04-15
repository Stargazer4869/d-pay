package com.dpay.sharding.core;

import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

public class MerchantShardLocator {

    private final JdbcTemplate controlJdbcTemplate;

    public MerchantShardLocator(JdbcTemplate controlJdbcTemplate) {
        this.controlJdbcTemplate = controlJdbcTemplate;
    }

    public String locateShardId(String merchantId) {
        return Optional.ofNullable(controlJdbcTemplate.query(
                        """
                        select shard_id
                        from platform.merchant_registry
                        where merchant_id = ?
                        """,
                        rs -> rs.next() ? rs.getString("shard_id") : null,
                        merchantId))
                .orElseThrow(() -> new IllegalArgumentException("Unknown merchant: " + merchantId));
    }
}
