package com.dpay.payment.config;

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
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(ShardRoutingProperties.class)
public class PaymentInfrastructureConfig {

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
    public ApplicationRunner paymentFlywayRunner(ShardRoutingProperties properties) {
        return args -> properties.getShards().values().forEach(connection ->
                Flyway.configure()
                        .dataSource(connection.getJdbcUrl(), connection.getUsername(), connection.getPassword())
                        .schemas("payment")
                        .defaultSchema("payment")
                        .table("payment_flyway_history")
                        .locations("classpath:db/migration/payment")
                        .load()
                        .migrate());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 5L)));
        return factory;
    }
}
