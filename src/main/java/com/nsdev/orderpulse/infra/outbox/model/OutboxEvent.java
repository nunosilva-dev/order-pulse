package com.nsdev.orderpulse.infra.outbox.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@Document(collection = "outbox_events")
public class OutboxEvent {

    @Id
    private String id;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String payload;
    private Instant createdAt;
    private OutboxStatus status;
    private int retryCount;
    private Instant lastAttempt;
    private String lastError;
}