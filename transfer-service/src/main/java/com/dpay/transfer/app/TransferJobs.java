package com.dpay.transfer.app;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class TransferJobs {

    private final TransferService transferService;

    public TransferJobs(TransferService transferService) {
        this.transferService = transferService;
    }

    @Scheduled(fixedDelayString = "${app.outbox.publish-delay-ms:500}")
    public void publishOutbox() {
        transferService.publishPendingOutbox();
    }

    @Scheduled(fixedDelayString = "${app.saga.reconcile-delay-ms:1000}")
    public void reconcileTransfers() {
        transferService.reconcileTransfers();
    }
}
