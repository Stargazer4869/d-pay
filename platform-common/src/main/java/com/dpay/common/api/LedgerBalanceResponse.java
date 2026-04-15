package com.dpay.common.api;

public record LedgerBalanceResponse(
        String merchantId,
        String currency,
        long availableBalance,
        long reservedOutgoing,
        long reservedRefund
) {
}
