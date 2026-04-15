package com.dpay.transfer.integration;

import com.dpay.common.api.OperationResult;
import com.dpay.common.api.TransferCreditRequest;
import com.dpay.common.api.TransferReserveRequest;
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

    public OperationResult reserveTransfer(TransferReserveRequest request) {
        return post("/internal/ledger/transfers/reservations", request, OperationResult.class);
    }

    public OperationResult finalizeTransfer(TransferReserveRequest request) {
        return post("/internal/ledger/transfers/finalizations", request, OperationResult.class);
    }

    public OperationResult releaseTransfer(TransferReserveRequest request) {
        return post("/internal/ledger/transfers/releases", request, OperationResult.class);
    }

    public OperationResult creditTransfer(TransferCreditRequest request) {
        return post("/internal/ledger/transfers/credits", request, OperationResult.class);
    }

    private <T> T post(String path, Object body, Class<T> type) {
        return restClient.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(type);
    }
}
