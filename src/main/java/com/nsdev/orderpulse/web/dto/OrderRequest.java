package com.nsdev.orderpulse.web.dto;

import java.math.BigDecimal;

public record OrderRequest(
        String customerName,
        String productSku,
        BigDecimal amount
) {
}