package com.nsdev.orderpulse.infra.outbox.repository;

import com.nsdev.orderpulse.infra.outbox.model.OutboxEvent;
import com.nsdev.orderpulse.infra.outbox.model.OutboxStatus;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OutboxRepository extends MongoRepository<OutboxEvent, String> {

    List<OutboxEvent> findByStatus(OutboxStatus status);
}