package com.nsdev.orderpulse.domain.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@Document(collection = "orders")
public class Order {

    @Id
    private String id;
    private String customerName;
    private String productSku;
    private BigDecimal amount;
    private OrderStatus status;
    private Instant createdAt;
}