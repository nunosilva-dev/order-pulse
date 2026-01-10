package com.nsdev.orderpulse;

import com.nsdev.orderpulse.domain.model.Order;
import com.nsdev.orderpulse.domain.model.OrderStatus;
import com.nsdev.orderpulse.domain.repository.OrderRepository;
import com.nsdev.orderpulse.infra.outbox.model.OutboxEvent;
import com.nsdev.orderpulse.infra.outbox.model.OutboxStatus;
import com.nsdev.orderpulse.infra.outbox.repository.OutboxRepository;
import com.nsdev.orderpulse.web.dto.OrderRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OrderE2ETest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxRepository outboxRepository;

    @BeforeEach
    void setup() {
        orderRepository.deleteAll();
        outboxRepository.deleteAll();
    }

    @Test
    @DisplayName("Should create order, persist outbox, and eventually send to Kafka")
    void shouldCreateOrderAndProcessEvent() {
        OrderRequest request = new OrderRequest("Test Client", "TEST-SKU-123", new BigDecimal("150.00"));

        ResponseEntity<Order> response = restTemplate.postForEntity(
                "/orders/create",
                request,
                Order.class
        );


        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        String orderId = response.getBody().getId();

        Order savedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(savedOrder.getCustomerName()).isEqualTo("Test Client");
        assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.CREATED);

        List<OutboxEvent> outboxEvents = outboxRepository.findAll();
        assertThat(outboxEvents).hasSize(1);

        OutboxEvent event = outboxEvents.getFirst();
        assertThat(event.getAggregateId()).isEqualTo(orderId);

        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    OutboxEvent updatedEvent = outboxRepository.findById(event.getId()).orElseThrow();

                    assertThat(updatedEvent.getStatus()).isEqualTo(OutboxStatus.COMPLETED);
                });

        System.out.println("✅ Test Passed: Order created -> Stored in Mongo -> Sent to Kafka -> Outbox marked COMPLETED");
    }

    @Test
    @DisplayName("Should maintain consistency: Invalid order should NOT persist Order nor Outbox event")
    void shouldRejectInvalidOrderAndMaintainConsistency() {
        OrderRequest invalidRequest = new OrderRequest("Bad Client", "SKU-FAIL", new BigDecimal("-100.00"));

        ResponseEntity<Object> response = restTemplate.postForEntity(
                "/orders/create",
                invalidRequest,
                Object.class
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(orderRepository.findAll()).isEmpty();
        assertThat(outboxRepository.findAll()).isEmpty();

        System.out.println("✅ Test Passed: Validation stopped data corruption. DB is clean.");
    }
}