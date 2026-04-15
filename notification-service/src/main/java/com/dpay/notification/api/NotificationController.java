package com.dpay.notification.api;

import com.dpay.notification.app.NotificationService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/webhooks")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @PutMapping("/{merchantId}")
    public void upsertSubscription(@PathVariable String merchantId, @Valid @RequestBody WebhookSubscriptionRequest request) {
        notificationService.registerSubscription(merchantId, request.targetUrl(), request.secret(), request.active());
    }
}
