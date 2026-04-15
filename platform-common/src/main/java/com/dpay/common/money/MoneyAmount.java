package com.dpay.common.money;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.Locale;

public record MoneyAmount(
        @Min(1) long amountMinor,
        @NotBlank String currency
) {

    public MoneyAmount {
        currency = currency.toUpperCase(Locale.ROOT);
    }
}
