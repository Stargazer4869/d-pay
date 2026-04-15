package com.dpay.payment.app;

import com.dpay.common.events.GatewayEvent;
import com.dpay.common.events.GatewayEventType;
import com.dpay.common.json.JsonUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventListener {

    private final PaymentProcessingService paymentProcessingService;

    public PaymentEventListener(PaymentProcessingService paymentProcessingService) {
        this.paymentProcessingService = paymentProcessingService;
    }

    @KafkaListener(topics = "${app.kafka.topic:gateway-events}", groupId = "${spring.application.name}")
    public void onMessage(String payload) {
        GatewayEvent event = JsonUtils.fromJson(payload, GatewayEvent.class);
        if (event.type() == GatewayEventType.PAYMENT_CONFIRM_REQUESTED || event.type() == GatewayEventType.REFUND_REQUESTED) {
            paymentProcessingService.handleEvent(event);
        }
    }
}
