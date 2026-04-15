package com.dpay.payment.integration;

import com.dpay.common.api.BankOperationRequest;
import com.dpay.common.api.BankOperationResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class BankHttpClient {

    private final RestClient restClient;

    public BankHttpClient(@Value("${app.integrations.bank.base-url}") String baseUrl) {
        this.restClient = RestClient.builder().baseUrl(baseUrl).build();
    }

    public BankOperationResponse withdraw(BankOperationRequest request) {
        return restClient.post()
                .uri("/bank/withdrawals")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(BankOperationResponse.class);
    }

    public BankOperationResponse deposit(BankOperationRequest request) {
        return restClient.post()
                .uri("/bank/deposits")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(BankOperationResponse.class);
    }
}
