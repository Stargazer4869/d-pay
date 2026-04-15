package com.dpay.payment.integration;

import com.dpay.common.api.OperationResult;
import com.dpay.common.api.PaymentLedgerCreditRequest;
import com.dpay.common.api.RefundLedgerRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class LedgerHttpClient {

    private final RestClient restClient;

    public LedgerHttpClient(@Value("${app.integrations.ledger.base-url}") String baseUrl) {
        this.restClient = RestClient.builder().baseUrl(baseUrl).build();
    }

    public OperationResult creditPayment(PaymentLedgerCreditRequest request) {
        return post("/internal/ledger/payments/credits", request);
    }

    public OperationResult reserveRefund(RefundLedgerRequest request) {
        return post("/internal/ledger/refunds/reservations", request);
    }

    public OperationResult finalizeRefund(RefundLedgerRequest request) {
        return post("/internal/ledger/refunds/finalizations", request);
    }

    public OperationResult releaseRefund(RefundLedgerRequest request) {
        return post("/internal/ledger/refunds/releases", request);
    }

    private OperationResult post(String path, Object body) {
        return restClient.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(OperationResult.class);
    }
}
