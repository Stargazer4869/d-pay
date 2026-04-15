package com.dpay.payment.app;

import com.dpay.payment.persistence.PaymentRepository;
import com.dpay.sharding.core.ShardRoutingManager;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PaymentJobs {

    private final ShardRoutingManager shardRoutingManager;
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public PaymentJobs(
            ShardRoutingManager shardRoutingManager,
            PaymentRepository paymentRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic:gateway-events}") String topic) {
        this.shardRoutingManager = shardRoutingManager;
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${app.outbox.publish-delay-ms:500}")
    public void publishOutbox() {
        shardRoutingManager.allShardContexts().values().forEach(context ->
                paymentRepository.findPendingOutboxEvents(context, 100).forEach(event -> {
                    kafkaTemplate.send(topic, event.get("aggregate_id").toString(), event.get("payload").toString());
                    paymentRepository.markOutboxPublished(context, UUID.fromString(event.get("id").toString()));
                }));
    }
}
