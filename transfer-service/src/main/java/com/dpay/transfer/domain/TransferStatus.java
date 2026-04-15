package com.dpay.transfer.domain;

public enum TransferStatus {
    REQUESTED,
    FUNDS_RESERVED,
    CREDITING,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED
}
