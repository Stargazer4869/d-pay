package com.dpay.notification.app;

import com.dpay.common.events.GatewayEvent;
import com.dpay.common.json.JsonUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationEventListener {

    private final NotificationService notificationService;

    public NotificationEventListener(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = "${app.kafka.topic:gateway-events}", groupId = "${spring.application.name}")
    public void onMessage(String payload) {
        notificationService.handleEvent(JsonUtils.fromJson(payload, GatewayEvent.class));
    }
}
