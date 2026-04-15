package com.dpay.notification.app;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class NotificationJobs {

    private final NotificationService notificationService;

    public NotificationJobs(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Scheduled(fixedDelayString = "${app.retry.scan-delay-ms:1000}")
    public void retryDueDeliveries() {
        notificationService.retryDueDeliveries();
    }
}
