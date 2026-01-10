package com.nsdev.orderpulse.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String customerName,
        BigDecimal amount,
        Instant occurredAt
) {
}