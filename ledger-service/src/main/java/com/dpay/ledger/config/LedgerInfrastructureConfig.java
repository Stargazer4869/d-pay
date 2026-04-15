package com.dpay.ledger.config;

import com.dpay.sharding.config.ShardRoutingProperties;
import com.dpay.sharding.core.MerchantShardLocator;
import com.dpay.sharding.core.ShardRoutingManager;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableConfigurationProperties(ShardRoutingProperties.class)
public class LedgerInfrastructureConfig {

    @Bean
    public DataSource controlDataSource(ShardRoutingProperties properties) {
        return ShardRoutingManager.createDataSource(properties.getControl());
    }

    @Bean
    public JdbcTemplate controlJdbcTemplate(DataSource controlDataSource) {
        return new JdbcTemplate(controlDataSource);
    }

    @Bean
    public MerchantShardLocator merchantShardLocator(JdbcTemplate controlJdbcTemplate) {
        return new MerchantShardLocator(controlJdbcTemplate);
    }

    @Bean
    public ShardRoutingManager shardRoutingManager(
            JdbcTemplate controlJdbcTemplate,
            MerchantShardLocator merchantShardLocator,
            ShardRoutingProperties properties) {
        return new ShardRoutingManager(
                controlJdbcTemplate,
                merchantShardLocator,
                ShardRoutingManager.buildShardContexts(properties.getShards()));
    }

    @Bean
    public ApplicationRunner ledgerFlywayRunner(ShardRoutingProperties properties) {
        return args -> properties.getShards().values().forEach(connection ->
                Flyway.configure()
                        .dataSource(connection.getJdbcUrl(), connection.getUsername(), connection.getPassword())
                        .schemas("ledger")
                        .defaultSchema("ledger")
                        .table("ledger_flyway_history")
                        .locations("classpath:db/migration/ledger")
                        .load()
                        .migrate());
    }
}
