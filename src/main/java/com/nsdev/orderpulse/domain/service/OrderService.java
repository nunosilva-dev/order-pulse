package com.nsdev.orderpulse.domain.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nsdev.orderpulse.domain.event.OrderCreatedEvent;
import com.nsdev.orderpulse.domain.exception.EventSerializationException;
import com.nsdev.orderpulse.domain.model.Order;
import com.nsdev.orderpulse.domain.model.OrderStatus;
import com.nsdev.orderpulse.domain.repository.OrderRepository;
import com.nsdev.orderpulse.infra.outbox.model.OutboxEvent;
import com.nsdev.orderpulse.infra.outbox.model.OutboxStatus;
import com.nsdev.orderpulse.infra.outbox.repository.OutboxRepository;
import com.nsdev.orderpulse.web.dto.OrderRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Service responsible for Order domain logic.
 * <p>
 * Implements the <strong>Transactional Outbox Pattern</strong> to ensure data consistency
 * between the MongoDB database and the Kafka message broker.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String AGGREGATE_TYPE_ORDER = "Order";
    private static final String EVENT_TYPE_ORDER_CREATED = "OrderCreated";

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new order and persists an outbox event in the same transaction.
     *
     * @param request The incoming order request data.
     * @return The persisted Order entity.
     * @throws EventSerializationException if JSON serialization of the event fails.
     */
    @Transactional
    public Order createOrder(OrderRequest request) {
        log.info("🛡️ Processing order for customer: {}", request.customerName());
        Order order = mapToOrder(request);
        Order savedOrder = orderRepository.save(order);
        OutboxEvent outboxEntry = createOutboxEvent(savedOrder);
        outboxRepository.save(outboxEntry);
        log.info("✅ Order created and Outbox event staged successfully. OrderID: {}", savedOrder.getId());
        return savedOrder;
    }

    private Order mapToOrder(OrderRequest request) {
        return Order.builder()
                .customerName(request.customerName())
                .productSku(request.productSku())
                .amount(request.amount())
                .status(OrderStatus.CREATED)
                .createdAt(Instant.now())
                .build();
    }

    private OutboxEvent createOutboxEvent(Order savedOrder) {
        OrderCreatedEvent eventPayload = new OrderCreatedEvent(
                savedOrder.getId(),
                savedOrder.getCustomerName(),
                savedOrder.getAmount(),
                savedOrder.getCreatedAt()
        );
        String jsonPayload;
        try {
            jsonPayload = objectMapper.writeValueAsString(eventPayload);
        } catch (JsonProcessingException e) {
            log.error("❌ Critical: Failed to serialize event payload for OrderID: {}", savedOrder.getId(), e);
            throw new EventSerializationException("Failed to serialize Outbox Event for Order: " + savedOrder.getId(), e);
        }
        return OutboxEvent.builder()
                .aggregateType(AGGREGATE_TYPE_ORDER)
                .aggregateId(savedOrder.getId())
                .eventType(EVENT_TYPE_ORDER_CREATED)
                .payload(jsonPayload)
                .createdAt(Instant.now())
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();
    }
}