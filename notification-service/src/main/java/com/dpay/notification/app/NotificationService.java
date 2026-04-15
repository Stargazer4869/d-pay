package com.dpay.notification.app;

import com.dpay.common.events.GatewayEvent;
import com.dpay.common.json.JsonUtils;
import com.dpay.common.web.WebhookSigner;
import com.dpay.notification.domain.WebhookDeliveryRecord;
import com.dpay.notification.persistence.NotificationRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Service
public class NotificationService {

    private final NotificationRepository notificationRepository;
    private final RestClient restClient;
    private final Duration retryDelay;
    private final int maxAttempts;

    public NotificationService(
            NotificationRepository notificationRepository,
            @Value("${app.retry.delay-seconds:2}") long retryDelaySeconds,
            @Value("${app.retry.max-attempts:5}") int maxAttempts) {
        this.notificationRepository = notificationRepository;
        this.restClient = RestClient.create();
        this.retryDelay = Duration.ofSeconds(retryDelaySeconds);
        this.maxAttempts = maxAttempts;
    }

    public void registerSubscription(String merchantId, String targetUrl, String secret, boolean active) {
        notificationRepository.upsertSubscription(merchantId, targetUrl, secret, active);
    }

    public void handleEvent(GatewayEvent event) {
        if (notificationRepository.isEventProcessed(event.eventId())) {
            return;
        }
        String payload = JsonUtils.toJson(event);
        targetMerchants(event).forEach(merchantId ->
                notificationRepository.findActiveSubscriptions(merchantId).forEach(subscription -> {
                    notificationRepository.upsertDelivery(
                            event.eventId(),
                            merchantId,
                            subscription.get("target_url").toString(),
                            subscription.get("secret").toString(),
                            payload);
                    attemptDelivery(new WebhookDeliveryRecord(
                            event.eventId(),
                            merchantId,
                            subscription.get("target_url").toString(),
                            subscription.get("secret").toString(),
                            payload,
                            "PENDING",
                            0,
                            null,
                            null,
                            Instant.now()));
                }));
        notificationRepository.markEventProcessed(event.eventId());
    }

    public void retryDueDeliveries() {
        notificationRepository.findDueDeliveries(Instant.now(), 100).forEach(this::attemptDelivery);
    }

    private void attemptDelivery(WebhookDeliveryRecord delivery) {
        int nextAttempt = delivery.attemptCount() + 1;
        try {
            int statusCode = restClient.post()
                    .uri(delivery.targetUrl())
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-DPay-Event-Id", delivery.eventId().toString())
                    .header("X-DPay-Signature", WebhookSigner.sign(delivery.secret(), delivery.payload()))
                    .body(delivery.payload())
                    .retrieve()
                    .toBodilessEntity()
                    .getStatusCode()
                    .value();

            if (statusCode >= 200 && statusCode < 300) {
                notificationRepository.updateDelivery(
                        delivery.eventId(),
                        delivery.merchantId(),
                        delivery.targetUrl(),
                        "SUCCESS",
                        nextAttempt,
                        statusCode,
                        null,
                        Instant.now());
                return;
            }

            scheduleRetry(delivery, nextAttempt, statusCode, "non-success status");
        } catch (RestClientException exception) {
            scheduleRetry(delivery, nextAttempt, null, exception.getMessage());
        }
    }

    private void scheduleRetry(WebhookDeliveryRecord delivery, int nextAttempt, Integer responseCode, String errorMessage) {
        String status = nextAttempt >= maxAttempts ? "FAILED" : "PENDING";
        Instant nextAttemptAt = nextAttempt >= maxAttempts ? Instant.now() : Instant.now().plus(retryDelay);
        notificationRepository.updateDelivery(
                delivery.eventId(),
                delivery.merchantId(),
                delivery.targetUrl(),
                status,
                nextAttempt,
                responseCode,
                errorMessage,
                nextAttemptAt);
    }

    private Set<String> targetMerchants(GatewayEvent event) {
        Set<String> merchants = new LinkedHashSet<>();
        if (event.merchantId() != null && !event.merchantId().isBlank()) {
            merchants.add(event.merchantId());
        }
        String destinationMerchantId = event.metadata().getOrDefault("destinationMerchantId", "");
        if (!destinationMerchantId.isBlank()) {
            merchants.add(destinationMerchantId);
        }
        return merchants;
    }
}
