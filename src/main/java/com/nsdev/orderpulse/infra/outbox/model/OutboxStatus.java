package com.nsdev.orderpulse.infra.outbox.model;

public enum OutboxStatus {
    PENDING,
    COMPLETED,
    FAILED,
    MANUAL_INTERVENTION
}