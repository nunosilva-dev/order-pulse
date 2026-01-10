package com.nsdev.orderpulse.warehouse;

import com.nsdev.orderpulse.domain.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
@Configuration
public class WarehouseConsumer {

    /**
     * Consumer bean that acts as the Warehouse Service.
     * <p>
     * Listens to the 'orders.events.v1' topic via Spring Cloud Stream.
     * It simulates inventory allocation logic.
     * <p>
     * The bean name 'warehouseProcessor' must match the configuration
     * in application.yml (spring.cloud.stream.function.definition).
     */
    @Bean
    public Consumer<Message<OrderCreatedEvent>> warehouseProcessor() {
        return message -> {
            OrderCreatedEvent event = message.getPayload();
            String messageId = Objects.requireNonNull(message.getHeaders().getId()).toString();
            Object originalEventId = message.getHeaders().get("eventId");
            log.info("📦 [Warehouse] Received Event. KafkaMsgID: {}, DomainEventID: {}", messageId, originalEventId);
            try {
                log.info("⚙️ [Warehouse] Allocating stock for Order: {} (SKU: {})", event.orderId(), event.customerName());
                simulateProcessingLatency();
                log.info("✅ [Warehouse] Stock allocated successfully for Order: {}", event.orderId());
            } catch (Exception e) {
                log.error("❌ [Warehouse] Failed to process order: {}", event.orderId());
                throw e;
            }
        };
    }

    /**
     * Simulates work (I/O, DB calls) to make the logs realistic.
     */
    private void simulateProcessingLatency() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}