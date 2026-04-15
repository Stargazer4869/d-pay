package com.dpay.ledger.domain;

public record LedgerAccount(
        String merchantId,
        String currency,
        long availableBalance,
        long reservedOutgoing,
        long reservedRefund
) {
}
