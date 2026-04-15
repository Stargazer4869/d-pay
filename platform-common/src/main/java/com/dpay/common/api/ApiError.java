package com.dpay.common.api;

import java.time.Instant;

public record ApiError(
        String code,
        String message,
        Instant timestamp
) {
}
