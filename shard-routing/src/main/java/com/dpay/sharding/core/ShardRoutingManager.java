package com.dpay.sharding.core;

import com.dpay.sharding.config.ShardRoutingProperties;
import com.zaxxer.hikari.HikariDataSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class ShardRoutingManager {

    private final JdbcTemplate controlJdbcTemplate;
    private final MerchantShardLocator merchantShardLocator;
    private final Map<String, ShardJdbcContext> shardContexts;

    public ShardRoutingManager(JdbcTemplate controlJdbcTemplate, MerchantShardLocator merchantShardLocator,
            Map<String, ShardJdbcContext> shardContexts) {
        this.controlJdbcTemplate = controlJdbcTemplate;
        this.merchantShardLocator = merchantShardLocator;
        this.shardContexts = shardContexts;
    }

    public JdbcTemplate controlJdbcTemplate() {
        return controlJdbcTemplate;
    }

    public String locateShard(String merchantId) {
        return merchantShardLocator.locateShardId(merchantId);
    }

    public ShardJdbcContext shardContextForMerchant(String merchantId) {
        return shardContextForShardId(locateShard(merchantId));
    }

    public ShardJdbcContext shardContextForShardId(String shardId) {
        ShardJdbcContext context = shardContexts.get(shardId);
        if (context == null) {
            throw new IllegalArgumentException("Unknown shard id: " + shardId);
        }
        return context;
    }

    public Map<String, ShardJdbcContext> allShardContexts() {
        return shardContexts;
    }

    public <T> T inShardTransactionForMerchant(String merchantId, Function<ShardJdbcContext, T> callback) {
        ShardJdbcContext context = shardContextForMerchant(merchantId);
        return context.transactionTemplate().execute(status -> callback.apply(context));
    }

    public <T> T inShardTransaction(String shardId, Function<ShardJdbcContext, T> callback) {
        ShardJdbcContext context = shardContextForShardId(shardId);
        return context.transactionTemplate().execute(status -> callback.apply(context));
    }

    public static DataSource createDataSource(ShardRoutingProperties.DatabaseConnectionProperties properties) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(properties.getJdbcUrl());
        dataSource.setUsername(properties.getUsername());
        dataSource.setPassword(properties.getPassword());
        dataSource.setDriverClassName(properties.getDriverClassName());
        return dataSource;
    }

    public static Map<String, ShardJdbcContext> buildShardContexts(
            Map<String, ShardRoutingProperties.DatabaseConnectionProperties> shardProperties) {
        Map<String, ShardJdbcContext> contexts = new LinkedHashMap<>();
        shardProperties.forEach((shardId, properties) -> {
            DataSource dataSource = createDataSource(properties);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
            TransactionTemplate transactionTemplate =
                    new TransactionTemplate(new DataSourceTransactionManager(dataSource));
            contexts.put(shardId, new ShardJdbcContext(shardId, jdbcTemplate, namedJdbcTemplate, transactionTemplate));
        });
        return contexts;
    }
}
