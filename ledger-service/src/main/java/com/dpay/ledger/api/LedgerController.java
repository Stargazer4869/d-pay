package com.dpay.ledger.api;

import com.dpay.common.api.LedgerBalanceResponse;
import com.dpay.common.api.OperationResult;
import com.dpay.common.api.PaymentLedgerCreditRequest;
import com.dpay.common.api.RefundLedgerRequest;
import com.dpay.common.api.TransferCreditRequest;
import com.dpay.common.api.TransferReserveRequest;
import com.dpay.ledger.app.LedgerService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/ledger")
public class LedgerController {

    private final LedgerService ledgerService;

    public LedgerController(LedgerService ledgerService) {
        this.ledgerService = ledgerService;
    }

    @PostMapping("/payments/credits")
    public OperationResult creditPayment(@Valid @RequestBody PaymentLedgerCreditRequest request) {
        return ledgerService.creditPayment(request);
    }

    @PostMapping("/refunds/reservations")
    public OperationResult reserveRefund(@Valid @RequestBody RefundLedgerRequest request) {
        return ledgerService.reserveRefund(request);
    }

    @PostMapping("/refunds/finalizations")
    public OperationResult finalizeRefund(@Valid @RequestBody RefundLedgerRequest request) {
        return ledgerService.finalizeRefund(request);
    }

    @PostMapping("/refunds/releases")
    public OperationResult releaseRefund(@Valid @RequestBody RefundLedgerRequest request) {
        return ledgerService.releaseRefund(request);
    }

    @PostMapping("/transfers/reservations")
    public OperationResult reserveTransfer(@Valid @RequestBody TransferReserveRequest request) {
        return ledgerService.reserveTransfer(request);
    }

    @PostMapping("/transfers/finalizations")
    public OperationResult finalizeTransfer(@Valid @RequestBody TransferReserveRequest request) {
        return ledgerService.finalizeTransfer(request);
    }

    @PostMapping("/transfers/releases")
    public OperationResult releaseTransfer(@Valid @RequestBody TransferReserveRequest request) {
        return ledgerService.releaseTransfer(request);
    }

    @PostMapping("/transfers/credits")
    public OperationResult creditTransfer(@Valid @RequestBody TransferCreditRequest request) {
        return ledgerService.creditTransfer(request);
    }

    @GetMapping("/balances/{merchantId}")
    public LedgerBalanceResponse getBalance(@PathVariable String merchantId, @RequestParam String currency) {
        return ledgerService.getBalance(merchantId, currency);
    }
}
