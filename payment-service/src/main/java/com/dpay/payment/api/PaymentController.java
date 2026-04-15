package com.dpay.payment.api;

import com.dpay.payment.app.PaymentService;
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
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping("/payments")
    public PaymentResponse createPayment(
            @Valid @RequestBody CreatePaymentRequest request,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return paymentService.createPayment(request, idempotencyKey);
    }

    @PostMapping("/payments/{paymentId}/confirm")
    public PaymentResponse confirmPayment(
            @PathVariable UUID paymentId,
            @RequestParam String merchantId,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return paymentService.confirmPayment(merchantId, paymentId, idempotencyKey);
    }

    @PostMapping("/payments/{paymentId}/cancel")
    public PaymentResponse cancelPayment(
            @PathVariable UUID paymentId,
            @RequestParam String merchantId,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return paymentService.cancelPayment(merchantId, paymentId, idempotencyKey);
    }

    @PostMapping("/payments/{paymentId}/refunds")
    public RefundResponse createRefund(
            @PathVariable UUID paymentId,
            @RequestParam String merchantId,
            @Valid @RequestBody CreateRefundRequest request,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return paymentService.createRefund(merchantId, paymentId, request, idempotencyKey);
    }

    @GetMapping("/payments/{paymentId}")
    public PaymentResponse getPayment(@PathVariable UUID paymentId, @RequestParam String merchantId) {
        return paymentService.getPayment(merchantId, paymentId);
    }

    @GetMapping("/payments")
    public List<PaymentResponse> listPayments(@RequestParam String merchantId) {
        return paymentService.listPayments(merchantId);
    }

    @GetMapping("/payments/{paymentId}/history")
    public List<HistoryItemResponse> getHistory(@PathVariable UUID paymentId, @RequestParam String merchantId) {
        return paymentService.getHistory(merchantId, paymentId);
    }
}
