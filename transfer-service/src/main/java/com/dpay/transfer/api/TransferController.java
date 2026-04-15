package com.dpay.transfer.api;

import com.dpay.transfer.app.TransferService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class TransferController {

    private final TransferService transferService;

    public TransferController(TransferService transferService) {
        this.transferService = transferService;
    }

    @PostMapping("/transfers")
    public TransferResponse createTransfer(
            @Valid @RequestBody CreateTransferRequest request,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return transferService.createTransfer(request, idempotencyKey);
    }

    @GetMapping("/transfers/{transferId}")
    public TransferResponse getTransfer(@PathVariable UUID transferId) {
        return transferService.getTransfer(transferId);
    }

    @GetMapping("/transfers/{transferId}/history")
    public List<TransferEventResponse> getHistory(@PathVariable UUID transferId) {
        return transferService.getHistory(transferId);
    }

    @GetMapping("/transfers")
    public List<TransferResponse> getTransfers(@RequestParam String merchantId) {
        return transferService.getTransfers(merchantId);
    }
}
