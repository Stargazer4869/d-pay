package com.dpay.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpay.bank.app.BankSimulatorServiceApplication;
import com.dpay.bank.domain.BankTransaction;
import com.dpay.common.api.LedgerBalanceResponse;
import com.dpay.ledger.app.LedgerServiceApplication;
import com.dpay.notification.api.WebhookSubscriptionRequest;
import com.dpay.notification.app.NotificationServiceApplication;
import com.dpay.payment.api.CreatePaymentRequest;
import com.dpay.payment.api.CreateRefundRequest;
import com.dpay.payment.api.HistoryItemResponse;
import com.dpay.payment.api.PaymentResponse;
import com.dpay.payment.api.RefundResponse;
import com.dpay.payment.app.PaymentServiceApplication;
import com.dpay.transfer.api.CreateTransferRequest;
import com.dpay.transfer.api.TransferEventResponse;
import com.dpay.transfer.api.TransferResponse;
import com.dpay.transfer.app.TransferServiceApplication;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestMethodOrder(OrderAnnotation.class)
class PaymentGatewayE2ETest {

    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:16-alpine");
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.6.1");
    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.2-alpine");

    private static PostgreSQLContainer<?> controlDb;
    private static PostgreSQLContainer<?> shard1Db;
    private static PostgreSQLContainer<?> shard2Db;
    private static KafkaContainer kafka;
    private static GenericContainer<?> redis;

    private static ConfigurableApplicationContext bankContext;
    private static ConfigurableApplicationContext ledgerContext;
    private static ConfigurableApplicationContext transferContext;
    private static ConfigurableApplicationContext notificationContext;
    private static ConfigurableApplicationContext paymentContext;

    private static RestClient paymentClient;
    private static RestClient transferClient;
    private static RestClient ledgerClient;
    private static RestClient notificationClient;
    private static RestClient bankClient;

    @BeforeAll
    static void setUp() throws Exception {
        configureTestcontainersForPodman();

        controlDb = new PostgreSQLContainer<>(POSTGRES_IMAGE).withDatabaseName("control").withUsername("dpay").withPassword("dpay");
        shard1Db = new PostgreSQLContainer<>(POSTGRES_IMAGE).withDatabaseName("shard1").withUsername("dpay").withPassword("dpay");
        shard2Db = new PostgreSQLContainer<>(POSTGRES_IMAGE).withDatabaseName("shard2").withUsername("dpay").withPassword("dpay");
        kafka = new KafkaContainer(KAFKA_IMAGE);
        redis = new GenericContainer<>(REDIS_IMAGE).withExposedPorts(6379);

        controlDb.start();
        shard1Db.start();
        shard2Db.start();
        kafka.start();
        redis.start();

        seedMerchantRegistryTable();

        bankContext = startBankService();
        ledgerContext = startLedgerService();
        transferContext = startTransferService();
        notificationContext = startNotificationService();
        paymentContext = startPaymentService();

        paymentClient = RestClient.builder().baseUrl(baseUrl(paymentContext)).build();
        transferClient = RestClient.builder().baseUrl(baseUrl(transferContext)).build();
        ledgerClient = RestClient.builder().baseUrl(baseUrl(ledgerContext)).build();
        notificationClient = RestClient.builder().baseUrl(baseUrl(notificationContext)).build();
        bankClient = RestClient.builder().baseUrl(baseUrl(bankContext)).build();
    }

    @AfterAll
    static void tearDown() {
        if (paymentContext != null) {
            paymentContext.close();
        }
        if (notificationContext != null) {
            notificationContext.close();
        }
        if (transferContext != null) {
            transferContext.close();
        }
        if (ledgerContext != null) {
            ledgerContext.close();
        }
        if (bankContext != null) {
            bankContext.close();
        }
        if (redis != null) {
            redis.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (shard2Db != null) {
            shard2Db.stop();
        }
        if (shard1Db != null) {
            shard1Db.stop();
        }
        if (controlDb != null) {
            controlDb.stop();
        }
    }

    @Test
    @Order(1)
    void createPaymentIsIdempotent() {
        String merchantId = registerMerchant("merchant-create", "shard1");

        CreatePaymentRequest request = new CreatePaymentRequest(
                merchantId,
                "order-create-1",
                1_000L,
                "USD",
                "payer-create-1",
                "payee-create-1",
                null);
        PaymentResponse first = createPayment(request, "idem-create-1");
        PaymentResponse second = createPayment(request, "idem-create-1");

        assertThat(second.id()).isEqualTo(first.id());
        assertThat(second.status()).isEqualTo("PENDING");
    }

    @Test
    @Order(2)
    void confirmPaymentSucceedsAndDuplicateConfirmDoesNotDoubleCredit() {
        String merchantId = registerMerchant("merchant-confirm", "shard1");
        PaymentResponse created = createPayment(new CreatePaymentRequest(
                merchantId,
                "order-confirm-1",
                2_500L,
                "USD",
                "payer-confirm-1",
                "payee-confirm-1",
                null), "idem-confirm-create");

        PaymentResponse confirmResponse = confirmPayment(created.id(), merchantId, "idem-confirm-1");
        PaymentResponse duplicateConfirm = confirmPayment(created.id(), merchantId, "idem-confirm-1");

        assertThat(confirmResponse.id()).isEqualTo(created.id());
        assertThat(duplicateConfirm.id()).isEqualTo(created.id());

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            PaymentResponse payment = getPayment(created.id(), merchantId);
            assertThat(payment.status()).isEqualTo("SUCCEEDED");
            assertThat(balance(merchantId, "USD").availableBalance()).isEqualTo(2_500L);
        });

        List<HistoryItemResponse> history = paymentHistory(created.id(), merchantId);
        assertThat(history).extracting(HistoryItemResponse::eventType)
                .contains("PAYMENT_CREATED", "PAYMENT_CONFIRM_REQUESTED", "PAYMENT_SUCCEEDED");
    }

    @Test
    @Order(3)
    void cancelPendingPaymentAndRejectConfirmAfterCancel() {
        String merchantId = registerMerchant("merchant-cancel", "shard1");
        PaymentResponse created = createPayment(new CreatePaymentRequest(
                merchantId,
                "order-cancel-1",
                700L,
                "USD",
                "payer-cancel-1",
                "payee-cancel-1",
                null), "idem-cancel-create");

        PaymentResponse canceled = cancelPayment(created.id(), merchantId, "idem-cancel-1");
        assertThat(canceled.status()).isEqualTo("CANCELED");

        assertThatThrownBy(() -> confirmPayment(created.id(), merchantId, "idem-cancel-confirm"))
                .isInstanceOf(HttpClientErrorException.Conflict.class);
    }

    @Test
    @Order(4)
    void transientBankFailuresEndInFailedPaymentWithoutDoubleCharge() {
        String merchantId = registerMerchant("merchant-transient-fail", "shard1");
        PaymentResponse created = createPayment(new CreatePaymentRequest(
                merchantId,
                "order-transient-fail-1",
                900L,
                "USD",
                "transient-withdraw-5",
                "payee-transient-fail-1",
                null), "idem-transient-create");

        confirmPayment(created.id(), merchantId, "idem-transient-confirm");

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            PaymentResponse payment = getPayment(created.id(), merchantId);
            assertThat(payment.status()).isEqualTo("FAILED");
            assertThat(balance(merchantId, "USD").availableBalance()).isZero();
        });

        BankTransaction transaction = bankClient.get()
                .uri("/bank/transactions/{id}", created.id())
                .retrieve()
                .body(BankTransaction.class);
        assertThat(transaction.attemptCount()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @Order(5)
    void partialAndFullRefundsAreIdempotentAndOverRefundIsRejected() {
        String merchantId = registerMerchant("merchant-refund", "shard1");
        PaymentResponse created = createPayment(new CreatePaymentRequest(
                merchantId,
                "order-refund-1",
                1_500L,
                "USD",
                "payer-refund-1",
                "payee-refund-1",
                null), "idem-refund-create");
        confirmPayment(created.id(), merchantId, "idem-refund-confirm");

        Awaitility.await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(getPayment(created.id(), merchantId).status()).isEqualTo("SUCCEEDED"));

        RefundResponse firstRefund = createRefund(created.id(), merchantId, new CreateRefundRequest(400L), "idem-refund-1");
        RefundResponse duplicateFirstRefund = createRefund(created.id(), merchantId, new CreateRefundRequest(400L), "idem-refund-1");
        assertThat(duplicateFirstRefund.id()).isEqualTo(firstRefund.id());

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            PaymentResponse payment = getPayment(created.id(), merchantId);
            assertThat(payment.status()).isEqualTo("PARTIALLY_REFUNDED");
            assertThat(payment.refundedAmountMinor()).isEqualTo(400L);
        });

        RefundResponse secondRefund = createRefund(created.id(), merchantId, new CreateRefundRequest(1_100L), "idem-refund-2");
        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            PaymentResponse payment = getPayment(created.id(), merchantId);
            assertThat(payment.status()).isEqualTo("REFUNDED");
            assertThat(payment.refundedAmountMinor()).isEqualTo(1_500L);
            assertThat(balance(merchantId, "USD").availableBalance()).isZero();
        });

        assertThat(secondRefund.status()).isEqualTo("PROCESSING");
        assertThatThrownBy(() -> createRefund(created.id(), merchantId, new CreateRefundRequest(1L), "idem-refund-over"))
                .isInstanceOf(HttpClientErrorException.Conflict.class);
    }

    @Test
    @Order(6)
    void crossShardTransferCompletesAndDuplicateTransferDoesNotDoubleMoveFunds() {
        String sourceMerchant = registerMerchant("merchant-transfer-source", "shard1");
        String destinationMerchant = registerMerchant("merchant-transfer-destination", "shard2");
        fundMerchant(sourceMerchant, 2_000L, "fund-source-transfer");

        TransferResponse created = createTransfer(new CreateTransferRequest(
                sourceMerchant,
                destinationMerchant,
                "transfer-1",
                750L,
                "USD"), "idem-transfer-1");
        TransferResponse duplicate = createTransfer(new CreateTransferRequest(
                sourceMerchant,
                destinationMerchant,
                "transfer-1",
                750L,
                "USD"), "idem-transfer-1");

        assertThat(duplicate.id()).isEqualTo(created.id());

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            TransferResponse transfer = getTransfer(created.id());
            assertThat(transfer.status()).isEqualTo("COMPLETED");
            assertThat(balance(sourceMerchant, "USD").availableBalance()).isEqualTo(1_250L);
            assertThat(balance(destinationMerchant, "USD").availableBalance()).isEqualTo(750L);
        });

        List<TransferEventResponse> history = transferHistory(created.id());
        assertThat(history).extracting(TransferEventResponse::eventType)
                .contains("TRANSFER_REQUESTED", "TRANSFER_FUNDS_RESERVED", "TRANSFER_CREDITING", "TRANSFER_COMPLETED");
    }

    @Test
    @Order(7)
    void destinationCreditFailureTriggersCompensationAndRestoresSourceBalance() {
        String sourceMerchant = registerMerchant("merchant-comp-source", "shard1");
        String destinationMerchant = registerMerchantExact("fail-credit-b", "shard2");
        fundMerchant(sourceMerchant, 1_800L, "fund-source-compensation");

        TransferResponse transfer = createTransfer(new CreateTransferRequest(
                sourceMerchant,
                destinationMerchant,
                "transfer-compensate",
                600L,
                "USD"), "idem-transfer-compensate");

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            TransferResponse state = getTransfer(transfer.id());
            assertThat(state.status()).isEqualTo("COMPENSATED");
            assertThat(balance(sourceMerchant, "USD").availableBalance()).isEqualTo(1_800L);
        });
    }

    @Test
    @Order(8)
    void retryableTransferCreditIsRecoveredByReconciliation() {
        String sourceMerchant = registerMerchant("merchant-retry-source", "shard1");
        String destinationMerchant = registerMerchantExact("retry-credit-2", "shard2");
        fundMerchant(sourceMerchant, 1_600L, "fund-source-retry");

        TransferResponse transfer = createTransfer(new CreateTransferRequest(
                sourceMerchant,
                destinationMerchant,
                "transfer-retry",
                500L,
                "USD"), "idem-transfer-retry");

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            TransferResponse state = getTransfer(transfer.id());
            assertThat(state.status()).isEqualTo("COMPLETED");
            assertThat(balance(sourceMerchant, "USD").availableBalance()).isEqualTo(1_100L);
            assertThat(balance(destinationMerchant, "USD").availableBalance()).isEqualTo(500L);
        });
    }

    @Test
    @Order(9)
    void webhookRetriesUseSameEventIdUntilSuccessfulDelivery() throws Exception {
        String merchantId = registerMerchant("merchant-webhook", "shard1");
        PaymentResponse created = createPayment(new CreatePaymentRequest(
                merchantId,
                "order-webhook-1",
                333L,
                "USD",
                "payer-webhook-1",
                "payee-webhook-1",
                null), "idem-webhook-create");

        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(500));
            server.enqueue(new MockResponse().setResponseCode(200));
            server.start();

            notificationClient.put()
                    .uri("/internal/webhooks/{merchantId}", merchantId)
                    .body(new WebhookSubscriptionRequest(server.url("/merchant").toString(), "secret-webhook", true))
                    .retrieve()
                    .toBodilessEntity();

            cancelPayment(created.id(), merchantId, "idem-webhook-cancel");

            Awaitility.await().atMost(Duration.ofSeconds(20))
                    .untilAsserted(() -> assertThat(server.getRequestCount()).isGreaterThanOrEqualTo(2));

            RecordedRequest first = server.takeRequest();
            RecordedRequest second = server.takeRequest();
            assertThat(first.getHeader("X-DPay-Event-Id")).isEqualTo(second.getHeader("X-DPay-Event-Id"));
            assertThat(first.getBody().readUtf8()).isEqualTo(second.getBody().readUtf8());
        }
    }

    private static ConfigurableApplicationContext startBankService() {
        return new SpringApplicationBuilder(BankSimulatorServiceApplication.class)
                .run(asArgs(bankProperties()));
    }

    private static ConfigurableApplicationContext startTransferService() {
        return new SpringApplicationBuilder(TransferServiceApplication.class)
                .run(asArgs(transferProperties()));
    }

    private static ConfigurableApplicationContext startNotificationService() {
        return new SpringApplicationBuilder(NotificationServiceApplication.class)
                .run(asArgs(notificationProperties()));
    }

    private static ConfigurableApplicationContext startPaymentService() {
        return new SpringApplicationBuilder(PaymentServiceApplication.class)
                .run(asArgs(paymentProperties()));
    }

    private static ConfigurableApplicationContext startLedgerService() {
        return new SpringApplicationBuilder(LedgerServiceApplication.class)
                .run(asArgs(ledgerProperties()));
    }

    private static Map<String, Object> singleDbProperties(PostgreSQLContainer<?> container) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.datasource.url", container.getJdbcUrl());
        properties.put("spring.datasource.username", container.getUsername());
        properties.put("spring.datasource.password", container.getPassword());
        return properties;
    }

    private static Map<String, Object> bankProperties() {
        Map<String, Object> properties = new LinkedHashMap<>(singleDbProperties(controlDb));
        properties.put("spring.application.name", "bank-simulator-service");
        properties.put("server.port", "0");
        properties.put("spring.flyway.default-schema", "bank");
        properties.put("spring.flyway.schemas", "bank");
        properties.put("spring.flyway.table", "bank_flyway_history");
        properties.put("spring.flyway.locations", "classpath:db/migration/bank");
        return properties;
    }

    private static Map<String, Object> transferProperties() {
        Map<String, Object> properties = new LinkedHashMap<>(singleDbProperties(controlDb));
        properties.put("spring.application.name", "transfer-service");
        properties.put("server.port", "0");
        properties.put("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
        properties.put("spring.flyway.default-schema", "transfer");
        properties.put("spring.flyway.schemas", "platform,transfer");
        properties.put("spring.flyway.table", "transfer_flyway_history");
        properties.put("spring.flyway.locations", "classpath:db/migration/transfer");
        properties.put("app.integrations.ledger.base-url", baseUrl(ledgerContext));
        properties.put("app.kafka.topic", "gateway-events");
        return properties;
    }

    private static Map<String, Object> notificationProperties() {
        Map<String, Object> properties = new LinkedHashMap<>(singleDbProperties(controlDb));
        properties.put("spring.application.name", "notification-service");
        properties.put("server.port", "0");
        properties.put("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
        properties.put("spring.flyway.default-schema", "notification");
        properties.put("spring.flyway.schemas", "platform,notification");
        properties.put("spring.flyway.table", "notification_flyway_history");
        properties.put("spring.flyway.locations", "classpath:db/migration/notification");
        properties.put("app.kafka.topic", "gateway-events");
        properties.put("spring.kafka.consumer.auto-offset-reset", "earliest");
        properties.put("spring.kafka.consumer.key-deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("spring.kafka.consumer.value-deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("spring.kafka.listener.missing-topics-fatal", "false");
        return properties;
    }

    private static Map<String, Object> paymentProperties() {
        Map<String, Object> properties = new LinkedHashMap<>(shardedProperties());
        properties.put("spring.application.name", "payment-service");
        properties.put("spring.flyway.enabled", "false");
        properties.put("server.port", "0");
        properties.put("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
        properties.put("spring.kafka.consumer.auto-offset-reset", "earliest");
        properties.put("spring.kafka.consumer.key-deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("spring.kafka.consumer.value-deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("spring.kafka.producer.key-serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("spring.kafka.producer.value-serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("spring.kafka.listener.missing-topics-fatal", "false");
        properties.put("app.integrations.ledger.base-url", baseUrl(ledgerContext));
        properties.put("app.integrations.bank.base-url", baseUrl(bankContext));
        properties.put("app.processing.max-attempts", "3");
        properties.put("app.kafka.topic", "gateway-events");
        return properties;
    }

    private static Map<String, Object> ledgerProperties() {
        Map<String, Object> properties = new LinkedHashMap<>(shardedProperties());
        properties.put("spring.application.name", "ledger-service");
        properties.put("spring.flyway.enabled", "false");
        properties.put("server.port", "0");
        return properties;
    }

    private static Map<String, Object> shardedProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("app.datasource.control.jdbc-url", controlDb.getJdbcUrl());
        properties.put("app.datasource.control.username", controlDb.getUsername());
        properties.put("app.datasource.control.password", controlDb.getPassword());
        properties.put("app.datasource.shards.shard1.jdbc-url", shard1Db.getJdbcUrl());
        properties.put("app.datasource.shards.shard1.username", shard1Db.getUsername());
        properties.put("app.datasource.shards.shard1.password", shard1Db.getPassword());
        properties.put("app.datasource.shards.shard2.jdbc-url", shard2Db.getJdbcUrl());
        properties.put("app.datasource.shards.shard2.username", shard2Db.getUsername());
        properties.put("app.datasource.shards.shard2.password", shard2Db.getPassword());
        return properties;
    }

    private static String[] asArgs(Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                .toArray(String[]::new);
    }

    private static String baseUrl(ConfigurableApplicationContext context) {
        return "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }

    private static void seedMerchantRegistryTable() throws Exception {
        try (Connection connection = DriverManager.getConnection(
                controlDb.getJdbcUrl(), controlDb.getUsername(), controlDb.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("create schema if not exists platform");
            statement.execute("""
                    create table if not exists platform.merchant_registry (
                        merchant_id varchar(128) primary key,
                        shard_id varchar(64) not null,
                        created_at timestamptz not null default now()
                    )
                    """);
        }
    }

    private static void configureTestcontainersForPodman() throws Exception {
        String socketPath = runCommand("podman", "machine", "inspect", "--format", "{{.ConnectionInfo.PodmanSocket.Path}}").trim();
        String dockerHost = "unix://" + socketPath;
        System.setProperty("docker.host", dockerHost);
        System.setProperty("DOCKER_HOST", dockerHost);
        System.setProperty("docker.client.strategy",
                "org.testcontainers.dockerclient.EnvironmentAndSystemPropertyClientProviderStrategy");
        System.setProperty("org.testcontainers.dockerclient.strategy",
                "org.testcontainers.dockerclient.EnvironmentAndSystemPropertyClientProviderStrategy");
        System.setProperty("testcontainers.ryuk.disabled", "true");
        System.setProperty("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", socketPath);
    }

    private static String runCommand(String... command) throws Exception {
        Process process = new ProcessBuilder(command).start();
        int exitCode = process.waitFor();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (exitCode != 0) {
            throw new IllegalStateException("Command failed: " + String.join(" ", command));
        }
        return output;
    }

    private static String registerMerchant(String prefix, String shardId) {
        String merchantId = prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        return registerMerchantExact(merchantId, shardId);
    }

    private static String registerMerchantExact(String merchantId, String shardId) {
        try (Connection connection = DriverManager.getConnection(
                controlDb.getJdbcUrl(), controlDb.getUsername(), controlDb.getPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     insert into platform.merchant_registry (merchant_id, shard_id, created_at)
                     values (?, ?, now())
                     on conflict (merchant_id) do update set shard_id = excluded.shard_id
                     """)) {
            statement.setString(1, merchantId);
            statement.setString(2, shardId);
            statement.executeUpdate();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        return merchantId;
    }

    private PaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {
        return paymentClient.post()
                .uri("/payments")
                .header("Idempotency-Key", idempotencyKey)
                .body(request)
                .retrieve()
                .body(PaymentResponse.class);
    }

    private PaymentResponse confirmPayment(UUID paymentId, String merchantId, String idempotencyKey) {
        return paymentClient.post()
                .uri(uriBuilder -> uriBuilder.path("/payments/{paymentId}/confirm")
                        .queryParam("merchantId", merchantId)
                        .build(paymentId))
                .header("Idempotency-Key", idempotencyKey)
                .retrieve()
                .body(PaymentResponse.class);
    }

    private PaymentResponse cancelPayment(UUID paymentId, String merchantId, String idempotencyKey) {
        return paymentClient.post()
                .uri(uriBuilder -> uriBuilder.path("/payments/{paymentId}/cancel")
                        .queryParam("merchantId", merchantId)
                        .build(paymentId))
                .header("Idempotency-Key", idempotencyKey)
                .retrieve()
                .body(PaymentResponse.class);
    }

    private RefundResponse createRefund(UUID paymentId, String merchantId, CreateRefundRequest request, String idempotencyKey) {
        return paymentClient.post()
                .uri(uriBuilder -> uriBuilder.path("/payments/{paymentId}/refunds")
                        .queryParam("merchantId", merchantId)
                        .build(paymentId))
                .header("Idempotency-Key", idempotencyKey)
                .body(request)
                .retrieve()
                .body(RefundResponse.class);
    }

    private PaymentResponse getPayment(UUID paymentId, String merchantId) {
        return paymentClient.get()
                .uri(uriBuilder -> uriBuilder.path("/payments/{paymentId}")
                        .queryParam("merchantId", merchantId)
                        .build(paymentId))
                .retrieve()
                .body(PaymentResponse.class);
    }

    private List<HistoryItemResponse> paymentHistory(UUID paymentId, String merchantId) {
        return paymentClient.get()
                .uri(uriBuilder -> uriBuilder.path("/payments/{paymentId}/history")
                        .queryParam("merchantId", merchantId)
                        .build(paymentId))
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    private TransferResponse createTransfer(CreateTransferRequest request, String idempotencyKey) {
        return transferClient.post()
                .uri("/transfers")
                .header("Idempotency-Key", idempotencyKey)
                .body(request)
                .retrieve()
                .body(TransferResponse.class);
    }

    private TransferResponse getTransfer(UUID transferId) {
        return transferClient.get()
                .uri("/transfers/{id}", transferId)
                .retrieve()
                .body(TransferResponse.class);
    }

    private List<TransferEventResponse> transferHistory(UUID transferId) {
        return transferClient.get()
                .uri("/transfers/{id}/history", transferId)
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    private LedgerBalanceResponse balance(String merchantId, String currency) {
        return ledgerClient.get()
                .uri(uriBuilder -> uriBuilder.path("/internal/ledger/balances/{merchantId}")
                        .queryParam("currency", currency)
                        .build(merchantId))
                .retrieve()
                .body(LedgerBalanceResponse.class);
    }

    private void fundMerchant(String merchantId, long amountMinor, String reference) {
        PaymentResponse payment = createPayment(new CreatePaymentRequest(
                merchantId,
                reference,
                amountMinor,
                "USD",
                "payer-" + reference,
                "payee-" + reference,
                null), "idem-" + reference + "-create");
        confirmPayment(payment.id(), merchantId, "idem-" + reference + "-confirm");
        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            PaymentResponse updated = getPayment(payment.id(), merchantId);
            assertThat(updated.status()).isEqualTo("SUCCEEDED");
        });
    }
}
