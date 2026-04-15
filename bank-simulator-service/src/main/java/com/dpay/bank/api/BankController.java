package com.dpay.bank.api;

import com.dpay.bank.app.BankTransactionService;
import com.dpay.bank.domain.BankTransaction;
import com.dpay.common.api.BankOperationRequest;
import com.dpay.common.api.BankOperationResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bank")
public class BankController {

    private final BankTransactionService transactionService;

    public BankController(BankTransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping("/withdrawals")
    public BankOperationResponse withdraw(@Valid @RequestBody BankOperationRequest request) {
        return transactionService.withdraw(request);
    }

    @PostMapping("/deposits")
    public BankOperationResponse deposit(@Valid @RequestBody BankOperationRequest request) {
        return transactionService.deposit(request);
    }

    @GetMapping("/transactions/{operationKey}")
    public BankTransaction getTransaction(@PathVariable String operationKey) {
        return transactionService.find(operationKey);
    }
}
